"""PDF presentation of a fully materialized attendance report; no database reads."""
from io import BytesIO
from pathlib import Path
from xml.sax.saxutils import escape
import unicodedata
import re

from django.http import HttpResponse
from django.utils import timezone
import reportlab
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak

from .services_extra_conciliacion import formato_minutos

VINO = colors.HexColor('#8B2252')


def _fonts():
    # These files ship in the pinned ReportLab wheel, including Docker builds.
    root = Path(reportlab.__file__).parent / 'fonts'
    for name, filename in [('AttendanceVera', 'Vera.ttf'), ('AttendanceVeraBold', 'VeraBd.ttf')]:
        if name not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont(name, str(root / filename)))


def _text(value):
    return escape(str(value if value is not None else 'N/D')).replace('\n', '<br/>')


def _clock(value):
    if value is None:
        return '-'
    if timezone.is_aware(value):
        value = timezone.localtime(value)
    return value.strftime('%H:%M')


def _meal(attendance):
    if attendance is None:
        return 'N/D'
    minutes = getattr(attendance, 'minutos_comida', None)
    return f'{minutes} min' if minutes is not None else 'N/D'


def _extra(summary):
    labels = [('detectado', 'Detectado'), ('autorizado', 'Autorizado'),
              ('pendiente', 'Diferencia pendiente'), ('rechazado', 'Rechazado'),
              ('solicitado', 'Solicitado pendiente')]
    text = ' | '.join(f'{label}: {formato_minutos(summary.get(key + "_minutos"))}' for key, label in labels)
    if summary.get('parcial'):
        text += ' | Subtotal parcial'
    text += (f' | Cobertura: {summary.get("dias_calculables", 0)} calculables; '
             f'{summary.get("dias_no_calculables", 0)} no calculables (incluye sin registro); '
             f'{summary.get("dias_sin_registro", 0)} sin registro; '
             f'{summary.get("dias_no_aplica", 0)} no aplica')
    return text


def _filename(data):
    departments = sorted({str(r['datos'].get('departamento') or 'sin-departamento') for r in data['reportes']})
    scope = '-'.join(departments) if departments else 'sin-empleados'
    if len(data['reportes']) == 1:
        scope += '-' + str(data['reportes'][0]['datos'].get('codigo') or data['reportes'][0]['datos']['id'])
    scope = unicodedata.normalize('NFKD', scope).encode('ascii', 'ignore').decode()
    scope = re.sub(r'[^A-Za-z0-9_-]+', '-', scope).strip('-')[:100] or 'reporte'
    return f'asistencia-{scope}-{data["fecha_inicio"]:%Y%m%d}-{data["fecha_fin"]:%Y%m%d}.pdf'


def exportar_pdf_reporte(data, *, titulo='Reporte de asistencia'):
    """Render the entire supplied scope, never just a screen's current page."""
    _fonts()
    normal = ParagraphStyle('Attendance', fontName='AttendanceVera', fontSize=7, leading=9,
                            spaceAfter=0, splitLongWords=True)
    bold = ParagraphStyle('AttendanceBold', parent=normal, fontName='AttendanceVeraBold')
    title = ParagraphStyle('AttendanceTitle', parent=bold, fontSize=16, leading=20, textColor=VINO)
    p = lambda value, style=normal: Paragraph(_text(value), style)
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=landscape(A4), leftMargin=28, rightMargin=28,
                           topMargin=30, bottomMargin=27, title=str(titulo), author="Pollyana's Dolce")
    story = [p(titulo, title), Spacer(1, 10)]
    departments = sorted({str(r['datos'].get('departamento') or 'Sin departamento') for r in data['reportes']})
    period = f'{data["fecha_inicio"]:%d/%m/%Y} al {data["fecha_fin"]:%d/%m/%Y}'
    stamp = timezone.localtime(data['consultado_en']).strftime('%d/%m/%Y %H:%M %Z')
    story += [p('Departamento: ' + ', '.join(departments)), p('Periodo: ' + period),
              p('Estado de los registros consultado: ' + stamp), Spacer(1, 10)]
    summary = data['resumen']
    story += [p(f'Empleados: {summary.get("empleados", len(data["reportes"]))} | Retardos registrados: {summary.get("retardos", 0)} | '
                f'Faltas pendientes: {summary.get("faltas", 0)} | Faltas conciliadas: {summary.get("faltas_conciliadas", 0)} | Faltas por retardos: {summary.get("falta_retardos", 0)}', bold),
              Spacer(1, 6), p(_extra(summary.get('extra', {}))), Spacer(1, 6)]
    permissions = summary.get('permisos', {})
    story += [p(f'Permisos aprobados: CG {formato_minutos(permissions.get("minutos_cg", 0))}, '
                f'{permissions.get("dias_cg", 0)} días | SG {formato_minutos(permissions.get("minutos_sg", 0))}, '
                f'{permissions.get("dias_sg", 0)} días | Conflictos de goce: {permissions.get("conflictos", 0)} | '
                f'Permisos con duración N/D: {permissions.get("duracion_desconocida", 0)}'),
              Spacer(1, 6), p(f'Antecedentes de 30 días: {data["ventana_inicio"]:%d/%m/%Y} al {data["fecha_fin"]:%d/%m/%Y}. '
                f'Faltas pendientes: {summary.get("faltas_30d", 0)} | Avisos/incidencias de baja: {summary.get("avisos_baja_30d", summary.get("avisos_baja", 0))}'),
              Spacer(1, 10), p('La asignación de departamento/área es la actual. Sin registro no significa falta. '
                'Base de extra: turno registrado o jornada de 8 h por duración, que incluye 35 min de comida. '
                'Se aplica la tolerancia del servicio existente y se descuenta solo comida excedente. '
                'Una detección parcial no autoriza pago. Consultar no modifica registros. '
                'Minutos tarde N/D si no existe turno. La falta por retardos se presenta separada del acumulado de faltas pendientes. '
                'Un aviso de baja no significa baja efectiva. CG: con goce; SG: sin goce.'), Spacer(1, 10)]
    if not data['reportes']:
        story.append(p('No hay empleados en el alcance consultado.'))
    widths = [52, 40, 40, 62, 36, 45, 49, 49, 49, 49, 82]
    widths.append(doc.width - sum(widths))
    headers = ['Fecha', 'Entrada', 'Salida', 'Comida min', 'Tarde min', 'Retardos reg.',
               'Extra det.', 'Extra aut.', 'Dif. pend.', 'Extra rech.', 'Permiso', 'Incidencias / estado / observaciones']
    for index, report in enumerate(data['reportes']):
        if index == 0:
            story.append(PageBreak())
        info, counts = report['datos'], report['resumen']
        heading = (f'{info.get("nombre", "")} | Código: {info.get("codigo", "")} | '
                   f'{info.get("departamento", "")} / {info.get("area", "")} | Sucursal: {info.get("sucursal", "")}')
        employee = report.get('empleado')
        if employee is not None:
            heading += ' | Estado actual: ' + ('Activo' if getattr(employee, 'activo', True) else 'Baja')
            baja = getattr(employee, 'fecha_baja_reporte', None)
            if baja is not None:
                heading += f' | Baja registrada: {baja:%d/%m/%Y}'
        detail = (f'Retardos registrados: {counts.get("retardos", 0)} | Faltas pendientes: {counts.get("faltas", 0)} | '
                  f'Faltas conciliadas: {counts.get("faltas_conciliadas", 0)} | Faltas por retardos: {counts.get("falta_retardos", 0)} | Faltas 30 días: {report.get("faltas_30d", 0)}. '
                  + _extra(report.get('extra_resumen', {}))
                  + f' | Permisos con duración N/D: {report.get("permisos_resumen", counts).get("duracion_desconocida", 0)}')
        if report.get('observaciones_resumen'):
            detail += '\n' + '\n'.join(str(note) for note in report['observaciones_resumen'])
        rows = [[p(heading, bold)] + [''] * 11, [p(detail)] + [''] * 11,
                [p(h, bold) for h in headers]]
        long_notes = []
        for row in report['filas']:
            attendance, extra = row.get('asistencia'), row.get('extra', {})
            incidents = []
            for incident in row.get('incidencias', []):
                incident_status = ' - '.join(str(incident.get(key) or '') for key in ('tipo', 'estado')).strip(' -')
                detail_text = str(incident.get('detalle') or '')
                if len(detail_text) > 100:
                    long_notes.append((row['fecha'], incident_status + ' | ' + detail_text))
                    incidents.append(incident_status + ' | Detalle en anexo')
                else:
                    incidents.append(incident_status + (' | ' + detail_text if detail_text else ''))
            status = row.get('estado_laboral_label', '')
            extra_status = extra.get('estado', '')
            explicit_missing = attendance is None and any(
                str(note).startswith(('Sin registro', 'Sin checada')) for note in row.get('observaciones', []))
            if explicit_missing and extra_status.startswith('No calculable'):
                extra_status = ''  # The original observation states the cause explicitly.
                if status in ('Activo', 'Inactivo'):
                    status = ''
            if status != 'No aplica' and extra_status:
                status = (status + ' | ' if status not in ('Activo', 'Inactivo') else '') + extra_status
            notes = [status] + incidents + list(row.get('observaciones', []))
            if extra.get('solicitado_minutos'):
                notes[0] += ' | Solicitado pendiente: ' + formato_minutos(extra['solicitado_minutos'])
            values = [row['fecha'].strftime('%d/%m/%Y'), _clock(getattr(attendance, 'entrada', None)),
                      _clock(getattr(attendance, 'salida', None)),
                      _meal(attendance),
                      row.get('tarde_minutos'), row.get('retardos_registrados', 0)]
            values += [formato_minutos(extra.get(key + '_minutos')) for key in ('detectado', 'autorizado', 'pendiente', 'rechazado')]
            notes_text = '\n'.join(str(n) for n in notes if n)
            permit_text = row.get('permiso_texto') or '-'
            if len(notes_text) > 350:
                long_notes.append((row['fecha'], notes_text))
                notes_text = '\n'.join([status] + [
                    ' - '.join(str(incident.get(key) or '') for key in ('tipo', 'estado')).strip(' -')
                    for incident in row.get('incidencias', [])] + ['Observaciones completas en anexo'])
            if len(permit_text) > 140:
                long_notes.append((row['fecha'], 'Permiso: ' + permit_text))
                permit_text = 'Ver permiso completo en anexo'
            values += [permit_text, notes_text]
            rows.append([p(value) for value in values])
        table = Table(rows, colWidths=widths, repeatRows=3, hAlign='LEFT', splitByRow=1, splitInRow=1)
        table.setStyle(TableStyle([
            ('SPAN', (0, 0), (-1, 0)), ('SPAN', (0, 1), (-1, 1)),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F8E8EE')),
            ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor('#F5F0EB')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'), ('GRID', (0, 2), (-1, -1), .25, colors.HexColor('#CBBCC1')),
            ('LEFTPADDING', (0, 0), (-1, -1), 3), ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 2), ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(table)
        annotations = report.get('avisos_30d', [])
        permits = report.get('permisos', [])
        if annotations or permits or long_notes:
            story.append(Spacer(1, 10))
            story.append(p('Anexo de permisos aprobados y antecedentes - ' + str(info.get('nombre', '')), bold))
            for note_date, note_text in long_notes:
                story.append(p('Detalle del día ' + note_date.isoformat(), bold))
                story.append(p(note_text))
                story.append(Spacer(1, 5))
            for permit in permits:
                interval = []
                for key in ('fecha_inicio', 'fecha_fin'):
                    value = permit.get(key)
                    interval.append(timezone.localtime(value).strftime('%d/%m/%Y %H:%M') if value else 'N/D')
                duration = (f'{permit.get("dias", 0)} días' if permit.get('dias')
                            else formato_minutos(permit.get('minutos')))
                story.append(p(f'Folio {permit.get("folio") or permit.get("id", "N/D")}: '
                               f'{permit.get("tipo", "")} | {permit.get("estado", "")} | '
                               f'{"CG" if permit.get("goce_sueldo") else "SG"} | '
                               f'{interval[0]} a {interval[1]} | Duración en periodo: {duration} | '
                               f'Motivo: {permit.get("motivo", "")}'))
                for observation in permit.get('observaciones', []):
                    story.append(p(observation))
            for notice in annotations:
                if isinstance(notice, dict):
                    notice = ' | '.join(str(notice.get(key) or '') for key in ('id', 'fecha', 'tipo', 'estado', 'detalle'))
                story.append(p('Aviso/incidencia de baja (no implica baja efectiva): ' + str(notice)))
            story.append(Spacer(1, 10))
    # A trailing spacer can otherwise create a blank final page.
    while story and isinstance(story[-1], Spacer):
        story.pop()
    def footer(canvas, document):
        canvas.saveState()
        canvas.setFont('AttendanceVera', 7)
        canvas.setFillColor(colors.HexColor('#7A6565'))
        canvas.drawString(28, 14, 'Pollyana\'s Dolce | RRHH | ' + period)
        canvas.drawRightString(landscape(A4)[0] - 28, 14, f'Página {document.page}')
        canvas.restoreState()
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    response = HttpResponse(output.getvalue(), content_type='application/pdf')
    response['Content-Disposition'] = f'attachment; filename="{_filename(data)}"'
    response['Cache-Control'] = 'private, no-store'
    response['X-Content-Type-Options'] = 'nosniff'
    return response
