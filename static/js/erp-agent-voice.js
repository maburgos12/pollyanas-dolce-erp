/* Native WAV capture keeps server validation and duration billing independent of browser codecs. */
(function (root) {
  function wav(chunks, rate) {
    const frames = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    if (!frames || rate < 8000 || rate > 48000 || frames > rate * 180) throw new Error('La nota excede tres minutos o no contiene audio.');
    const data = new DataView(new ArrayBuffer(44 + frames * 2));
    function label(offset, text) { for (let i=0;i<text.length;i++) data.setUint8(offset+i, text.charCodeAt(i)); }
    label(0,'RIFF'); data.setUint32(4,36+frames*2,true); label(8,'WAVE'); label(12,'fmt ');
    data.setUint32(16,16,true); data.setUint16(20,1,true); data.setUint16(22,1,true);
    data.setUint32(24,rate,true); data.setUint32(28,rate*2,true); data.setUint16(32,2,true); data.setUint16(34,16,true);
    label(36,'data'); data.setUint32(40,frames*2,true);
    let offset=44;
    for (const chunk of chunks) for (const sample of chunk) {
      const value = Math.max(-1, Math.min(1, sample));
      data.setInt16(offset, Math.round(value * (value < 0 ? 32768 : 32767)), true); offset+=2;
    }
    return new Blob([data], {type:'audio/wav'});
  }
  function attach({button, input, preview, remove, status, worklet, onRecording}) {
    let file=null, url=null, recording=null, starting=false, busy=false, closed=false;
    function controls() {
      button.disabled=busy || starting;
      input.disabled=busy || !!recording || starting;
      remove.disabled=busy || !!recording || starting || !file;
    }
    function clear() {
      if (url) URL.revokeObjectURL(url);
      file=null; url=null; input.value=''; preview.removeAttribute('src'); preview.hidden=true; controls();
    }
    function select(value) {
      clear(); file=value; url=URL.createObjectURL(file); preview.src=url; preview.hidden=false;
      status.textContent='Nota lista. Puedes escucharla y enviar junto con texto o imágenes.'; controls();
    }
    input.addEventListener('change', () => {
      const selected=input.files[0];
      if (!selected) return;
      if (!/\.wav$/i.test(selected.name) || selected.size>8*1024*1024) {
        clear(); status.textContent='Adjunta WAV de 16 bits, hasta 3 minutos y 8 MB, o graba aquí.'; return;
      }
      select(selected);
    });
    remove.addEventListener('click', () => {clear(); status.textContent='Nota retirada.';});
    async function stop(keep=true) {
      const current=recording;
      if (!current) return;
      recording=null; clearTimeout(current.timer); current.stream.getTracks().forEach(track=>track.stop());
      current.source.disconnect(); current.processor.disconnect(); current.processor.port.close();
      if(current.context.state!=='closed') await current.context.close();
      button.textContent='Grabar nota'; button.setAttribute('aria-pressed','false'); onRecording(false);
      try { if (keep) select(new File([wav(current.chunks,current.rate)],'nota-de-voz.wav',{type:'audio/wav'})); }
      catch(error) {status.textContent=error.message;}
      controls();
    }
    button.addEventListener('click', async () => {
      if (recording) {await stop(); return;}
      if (starting || busy) return;
      starting=true; onRecording(true); controls();
      let stream=null, context=null;
      try {
        if (!navigator.mediaDevices?.getUserMedia || !root.AudioWorkletNode) throw new Error('Este navegador no permite grabar. Puedes adjuntar una nota WAV.');
        context=new AudioContext({sampleRate:16000}); await context.resume();
        stream=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:true},video:false});
        await context.audioWorklet.addModule(worklet);
        if(document.hidden || closed) throw new Error("La grabación se canceló al salir de la pestaña.");
        if (context.sampleRate<8000 || context.sampleRate>48000) throw new Error('Frecuencia de audio no compatible. Adjunta una nota WAV.');
        clear();
        const current={context,stream,rate:context.sampleRate,chunks:[],frames:0,source:context.createMediaStreamSource(stream),processor:new AudioWorkletNode(context,'erp-voice-input')};
        recording=current;
        current.processor.port.onmessage=event => {
          if (recording!==current) return;
          const chunk=event.data;
          if (current.frames+chunk.length>current.rate*180 || (current.frames+chunk.length)*2+44>8*1024*1024) {stop(); return;}
          current.chunks.push(chunk); current.frames+=chunk.length;
        };
        const mute=context.createGain(); mute.gain.value=0;
        current.source.connect(current.processor); current.processor.connect(mute); mute.connect(context.destination);
        current.timer=setTimeout(()=>stop(),180000);
        button.textContent='Detener grabación'; button.setAttribute('aria-pressed','true');
        status.textContent='Grabando. Pulsa Detener grabación al terminar (máximo 3 minutos).'; onRecording(true);
      } catch(error) {
        stream?.getTracks().forEach(track=>track.stop()); if(context && context.state!=='closed') await context.close();
        status.textContent=error.name==='NotAllowedError' ? 'El micrófono no tiene permiso. Puedes autorizarlo en el navegador o adjuntar WAV.' : error.message;
      } finally {starting=false; if(!recording) onRecording(false); controls();}
    });
    document.addEventListener('visibilitychange', () => {if(document.hidden) stop();});
    root.addEventListener('pagehide', () => {closed=true; stop(false); if(url) URL.revokeObjectURL(url);});
    return {get file(){return file;}, clear, setBusy(value){busy=value; controls();}};
  }
  root.ERPVoice={wav,attach};
  if (typeof module!=='undefined') module.exports={wav};
})(typeof window==='undefined' ? globalThis : window);
