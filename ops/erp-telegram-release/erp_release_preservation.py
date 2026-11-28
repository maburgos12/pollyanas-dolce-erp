"""Pending-wire integrity only. No task execution or semantics by task name."""
import base64,copy,hashlib,json,math
MAX_UNACKED=32
MAX_READY=2000
MAX_RAW=16384
RAW_BUDGET=4194304
OUTPUT_LIMIT=8388608
QUEUES=('celery','notificaciones','sat','recipes','logistica_point','hours_control','hours_publish')
QUEUE_KEYS={q if p==0 else q+'\x06\x16'+str(p) for q in QUEUES for p in (0,3,6,9)}

class NeedsReview(AssertionError):pass

def require(condition,message):
    if not condition:raise NeedsReview(message)

def unique_object(pairs):
    result={}
    for key,value in pairs:
        require(key not in result,'Ambiguous duplicate JSON key');result[key]=value
    return result

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def decode_raw(record):
    encoded=record['raw_base64']
    require(isinstance(encoded,str) and len(encoded)<=4*((MAX_RAW+2)//3),'Encoded raw cap exceeded')
    raw=base64.b64decode(encoded,validate=True)
    require(len(raw)<=MAX_RAW,'Per-message raw cap exceeded')
    return raw

def packet(record):
    raw=decode_raw(record)
    require(len(raw)<=MAX_RAW and hashlib.sha256(raw).hexdigest()==record['sha256'],'Raw backup/hash differs')
    value=json.loads(raw,object_pairs_hook=unique_object)
    if record['location']=='unacked':
        require(isinstance(value,list) and len(value)==3,'Unexpected reservation envelope')
        message=value[0];route=value[2]
        require(value[1]=='','Unknown reservation exchange; binding requires classification')
        require(isinstance(record['tag'],str) and 0<len(record['tag'])<=256 and type(record['score']) in (int,float) and math.isfinite(record['score']),'Invalid reservation identity/score')
    else:
        require(record['location']=='ready' and record['queue'] in QUEUE_KEYS,'Ready queue outside captured scope')
        message=value;route=record['queue'].split('\x06\x16')[0]
    require(isinstance(message,dict) and isinstance(message.get('headers'),dict) and isinstance(message.get('properties'),dict),'Invalid transport message')
    require(isinstance(message.get('body'),str),'Transport body must stay an opaque encoded string')
    require(isinstance(message['properties'].get('delivery_info'),dict),'Cannot prove native restoration of malformed delivery_info')
    task_id=message['headers'].get('id')
    require(isinstance(task_id,str) and 0<len(task_id)<=256 and record['id']==task_id,'Missing/ambiguous message identity')
    require(isinstance(route,str) and route in QUEUES and record['route']==route,'Descriptor route differs from raw')
    info=message['properties']['delivery_info']
    require(info.get('exchange')=='' and info.get('routing_key')==route,'Unknown exchange or ambiguous inner/outer routing key')
    canonical(message) # Reject nonfinite JSON values, without interpreting body.
    return message,raw

def normalized(message):
    value=copy.deepcopy(message)
    value['headers'].pop('redelivered',None)
    value['properties']['delivery_info'].pop('redelivered',None)
    return canonical(value)

def expected_ready_queue(record,message):
    # Same clamping and priority steps as the byte-verified Kombu Redis transport.
    route=record['route']
    if route not in QUEUES:return None
    try:priority=min(max(int(message['properties'].get('priority',0)),0),9)
    except (TypeError,ValueError):priority=0
    step=max(p for p in (0,3,6,9) if p<=priority)
    return route if step==0 else route+'\x06\x16'+str(step)

def coverage_violations(value):
    pending=sum(r['location']=='unacked' for r in value['records'])
    violations=[]
    for key in ('unacked_count_hint_before','unacked_count_hint_after'):
        observed=value.get(key)
        if type(observed) is not int or observed!=pending:
            violations.append({'guard':key,'observed':observed,'captured_unique':pending})
    hints=value.get('ready_length_hints');lengths=value.get('ready_lengths')
    if (not isinstance(hints,dict) or set(hints)!=QUEUE_KEYS
        or any(type(n) is not int or n<0 for n in hints.values())
        or sum(hints.values())>MAX_READY or hints!=lengths):
        violations.append({'guard':'ready_count_coverage','hint_counts':hints,'captured_counts':lengths})
    return violations

def validate(value):
    rows=value['records'];require(isinstance(rows,list),'Records must be a list')
    require(not coverage_violations(value),'Capture coverage changed during interval; review archived cut')
    pending=[r for r in rows if r['location']=='unacked'];ready=[r for r in rows if r['location']=='ready']
    require(len(pending)<=MAX_UNACKED and len(ready)<=MAX_READY,'Delivery count bound exceeded')
    # All byte bounds are checked before decoding any packet.
    sizes=[len(decode_raw(r)) for r in rows]
    require(all(n<=MAX_RAW for n in sizes) and sum(sizes)<=RAW_BUDGET,'Raw budget exceeded')
    require(set(value['ready_lengths'])==QUEUE_KEYS,'Incomplete queue coverage')
    require(all(type(n) is int and n>=0 for n in value['ready_lengths'].values()),'Invalid queue counts')
    require(value['ready_lengths']=={q:sum(r['queue']==q for r in ready) for q in QUEUE_KEYS},'Captured per-queue count differs')
    ids={};tags=set()
    for r in rows:
        packet(r)
        require(r['id'] not in ids,'Duplicate logical identity across captured locations')
        ids[r['id']]=r
        if r['location']=='unacked':
            require(r['tag'] not in tags,'Duplicate reservation tag');tags.add(r['tag'])
            require(value['tag_states'].get(r['tag'])=={'hash_present':True,'score':r['score']},'Reservation changed during capture interval')
    return ids

def validate_before(value):return validate(value)

def assess(before,after,*,scope='general',all_stopped=False):
    old=validate(before);new=validate(after);outcomes={};transitions=[];births=[]
    other_progress=[]
    controlled={'celery'} if scope=='general' else set(QUEUES)
    require(scope in ('general','all'),'Invalid consumer scope')
    for task_id,prior in old.items():
        current=new.get(task_id)
        if prior['route'] not in controlled:
            if current!=prior:other_progress.append({'id':task_id,'observation':'outside_stopped_consumer_scope_changed','no_success_inference':True})
            if current is not None and current['route'] in controlled:transitions.append({'id':task_id,'reason':'route_changed_into_stopped_lane_requires_classification'})
            continue
        reason=None
        if current is None:reason='missing_requires_terminal_or_transition_evidence'
        elif prior['location']=='unacked' and current['location']=='unacked':
            if all(prior[k]==current[k] for k in ('tag','score','sha256','raw_base64')):outcomes[task_id]='reserved_exact'
            else:reason='reservation_changed'
        elif prior['location']=='ready' and current['location']=='ready':
            if all(prior[k]==current[k] for k in ('queue','sha256','raw_base64')):outcomes[task_id]='ready_exact'
            else:reason='initial_ready_changed'
        elif prior['location']=='unacked' and current['location']=='ready':
            a,_=packet(prior);b,_=packet(current)
            tag_state=after['tag_states'].get(prior['tag'])
            if (normalized(a)==normalized(b) and current['queue']==expected_ready_queue(prior,a)
                and b['headers'].get('redelivered') is True and b['properties']['delivery_info'].get('redelivered') is True
                and tag_state=={'hash_present':False,'score':None}):outcomes[task_id]='restored_unique_exact'
            else:reason='restoration_differs_or_old_tag_remains'
        else:reason='location_transition_requires_review'
        if reason:transitions.append({'id':task_id,'reason':reason})
    for task_id,current in new.items():
        if task_id in old:continue
        birth={'id':task_id,'location':current['location'],'route':current['route'],'sha256':current['sha256']}
        births.append(birth)
        if current['location']=='unacked' and ((all_stopped and current['route'] in QUEUES) or current['route']=='celery'):
            transitions.append({'id':task_id,'reason':'new_reservation_after_consumer_stop_requires_classification'})
    return {'preserved':not transitions,'outcomes':outcomes,'transitions':transitions,'new_pending':births,'other_consumer_progress':other_progress,'no_task_success_inferred':True}

def validate_restored(before,after,*,scope='general',all_stopped=False):
    result=assess(before,after,scope=scope,all_stopped=all_stopped)
    require(result['preserved'],'Pending-set transition requires supervised review')
    return result

def packet_changes(a,b,path=''):
    if canonical(a)==canonical(b):return []
    if isinstance(a,dict) and isinstance(b,dict):
        rows=[]
        for key in sorted(set(a)|set(b)):
            if key not in a or key not in b:rows.append({'path':path+'/'+key,'before_present':key in a,'after_present':key in b})
            else:rows.extend(packet_changes(a[key],b[key],path+'/'+key))
        return rows
    def summary(value):
        encoded=canonical(value).encode()
        if path not in ('/body','/headers/argsrepr','/headers/kwargsrepr') and not isinstance(value,(dict,list)) and len(encoded)<=256:return {'value':value,'type':type(value).__name__}
        return {'sha256':hashlib.sha256(encoded).hexdigest(),'json_bytes':len(encoded),'full_value_in_raw_snapshot':True}
    return [{'path':path,'before':summary(a),'after':summary(b)}]

def descriptor_diff(old,new):
    a=old['records'];b=new['records'];by_a={r['id']:r for r in a};by_b={r['id']:r for r in b};result=[]
    for task_id in sorted(set(by_a)|set(by_b)):
        prior=by_a.get(task_id);current=by_b.get(task_id)
        if prior==current:continue
        # Raw remains solely in the two immutable snapshots, referenced by ID/hash.
        row={'id':task_id,'before':None if prior is None else {k:v for k,v in prior.items() if k!='raw_base64'},'after':None if current is None else {k:v for k,v in current.items() if k!='raw_base64'}}
        if prior and current:
            am,_=packet(prior);bm,_=packet(current)
            row['packet_changes']=packet_changes(am,bm)
            row['eta_before']=am['headers'].get('eta');row['eta_after']=bm['headers'].get('eta')
            row['retries_before']=am['headers'].get('retries');row['retries_after']=bm['headers'].get('retries')
        result.append(row)
    return result
