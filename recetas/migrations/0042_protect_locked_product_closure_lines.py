from django.db import migrations


CREATE_GUARD_SQL = r"""
CREATE FUNCTION recetas_guard_locked_product_closure_line()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    parent_is_locked boolean;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT is_locked
          INTO parent_is_locked
          FROM recetas_productomonthclosure
         WHERE id = NEW.closure_id
         FOR KEY SHARE;

        IF COALESCE(parent_is_locked, false) THEN
            RAISE EXCEPTION
                'No se puede insertar una línea en el cierre mensual bloqueado (id=%).',
                NEW.closure_id
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        SELECT is_locked
          INTO parent_is_locked
          FROM recetas_productomonthclosure
         WHERE id = OLD.closure_id
         FOR KEY SHARE;

        IF COALESCE(parent_is_locked, false) THEN
            RAISE EXCEPTION
                'No se puede eliminar una línea del cierre mensual bloqueado (id=%).',
                OLD.closure_id
                USING ERRCODE = '55000';
        END IF;
        RETURN OLD;
    END IF;

    SELECT is_locked
      INTO parent_is_locked
      FROM recetas_productomonthclosure
     WHERE id = OLD.closure_id
     FOR KEY SHARE;

    IF COALESCE(parent_is_locked, false) THEN
        RAISE EXCEPTION
            'No se puede modificar una línea del cierre mensual bloqueado (id=%).',
            OLD.closure_id
            USING ERRCODE = '55000';
    END IF;

    IF NEW.closure_id IS DISTINCT FROM OLD.closure_id THEN
        SELECT is_locked
          INTO parent_is_locked
          FROM recetas_productomonthclosure
         WHERE id = NEW.closure_id
         FOR KEY SHARE;

        IF COALESCE(parent_is_locked, false) THEN
            RAISE EXCEPTION
                'No se puede mover una línea al cierre mensual bloqueado (id=%).',
                NEW.closure_id
                USING ERRCODE = '55000';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER recetas_protect_locked_product_closure_line
BEFORE INSERT OR UPDATE OR DELETE
ON recetas_productomonthclosureline
FOR EACH ROW
EXECUTE FUNCTION recetas_guard_locked_product_closure_line();
"""


DROP_GUARD_SQL = r"""
DROP TRIGGER IF EXISTS recetas_protect_locked_product_closure_line
ON recetas_productomonthclosureline;
DROP FUNCTION IF EXISTS recetas_guard_locked_product_closure_line();
"""


class Migration(migrations.Migration):
    dependencies = [
        ("recetas", "0041_receta_grupo_mano_obra"),
    ]

    operations = [
        migrations.RunSQL(
            sql=CREATE_GUARD_SQL,
            reverse_sql=DROP_GUARD_SQL,
        ),
    ]
