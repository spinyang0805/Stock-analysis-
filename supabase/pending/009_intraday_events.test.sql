-- checks run inside the rolled-back validation transaction
select public.tw_round(123.37, false, 'down') as r1, public.tw_round(55.03, true, 'up') as r2, public._vol_profile('10:00') as vp;
select public._event_text('2330','台積電','pick',false,1500,1480,null,1490,1450,1400,1495,3,25,1488,null,array['breakout']) as t1;
select public._event_text('2330','台積電','holding',false,1440,1480,1450,1490,1450,1400,1460,3,25,1438,null,array['stop','support_break']) as t2;
select public.intraday_events() as n;
select public._flush_outbox(false);
