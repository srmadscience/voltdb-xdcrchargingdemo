select currentclusterid c, seqno , EVENTTIME,XDCRROWTYPE,XDCRACTIONTYPE,XDCRCONFLICTTYPE, WASACCEPTED, CONFLICTTIMESTSTAMP, TUPLEJSON, inserttime from xdcr_conflicts 
where tuplejson like '%"USERID"_"62970"%' 
and tablename = 'USER_TABLE'
order by currentclusterid, seqno;
