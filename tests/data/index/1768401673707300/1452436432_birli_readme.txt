This run succeded at: 14-01-2026 22:38:39
Command: /software/birli/target/release/birli --metafits /data/calvin/jobs/3692_1452436432/1452436432_metafits.fits --no-draw-progress --uvfits-out=/tmp/calvin/jobs/3692_1452436432/1452436432.uvfits --flag-edge-width=0 --max-memory=805  /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch131_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch150_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch143_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch151_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch137_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch154_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch148_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch135_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch149_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch136_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch139_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch133_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch152_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch145_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch134_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch147_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch141_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch138_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch140_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch153_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch142_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch132_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch146_000.fits /data/calvin/jobs/3692_1452436432/1452436432_20260114143334_ch144_000.fits 
Exit code: 0
stdout: 
stderr: [2026-01-14T14:37:52Z INFO  birli::cli] Using default MWA array position.
[2026-01-14T14:37:52Z INFO  birli::cli] cable corrections: applied=NoCableDelaysApplied, disabled=false
[2026-01-14T14:37:52Z INFO  birli::cli] passband gains: auto (disabled, deripple already applied)
[2026-01-14T14:37:52Z INFO  birli::cli] geometric corrections: applied=No, disabled=false
[2026-01-14T14:37:52Z INFO  birli::cli] birli version 0.18.2
    Compiled on git commit hash: 1400650e69c32ac1dac8b9df0c57da4b08e58873 (dirty)
                git head ref: refs/heads/main
                Mon, 13 Oct 2025 06:45:55 +0000
             with compiler rustc 1.89.0 (29483883e 2025-08-04)
    libraries:
    - marlu v0.16.1
    - mwalib v1.8.7
    - aoflagger v3.4.0
    
    observation name:     high_3C161_2025A_2461054_RADec96.8,-5.9_Ch143
    Array position:       { longitude: 116.6708°, latitude: -26.7033°, height: 377.827m }
    Phase centre:         (96.7913°, -5.8850°) => (6h27m09.9120s, -5d53m05.9999s)
    Pointing centre:      (95.9089°, -5.6603°) => (6h23m38.1354s, -5d39m37.0029s)
    Scheduled start:      2026-01-14 14:33:34.000 UTC, unix=1768401214.000, gps=1452436432.000, mjd=5275118014.000, lmst=89.1347°, lmst2k=88.8727°, lat2k=-26.7081°
    Scheduled end:        2026-01-14 14:35:34.000 UTC, unix=1768401334.000, gps=1452436552.000, mjd=5275118134.000, lmst=89.6360°, lmst2k=89.3741°, lat2k=-26.7069°
    Scheduled duration:   120.000s =  60 * 2.000s
    Quack duration:       2.000s =   1 * 2.000s
    Output duration:      120.000s =  60 * 2.000s
    Scheduled Bandwidth:  30.720MHz =  24 *  32 * 40.000kHz
    Output Bandwidth:     30.720MHz =       768 * 40.000kHz
    Timestep details (all=60, provided=60, common=60, good=59, select=60, flag=1):
            2026-01-14 UTC +  unix [s]        gps [s]         p  c  g  s  f 
      ts0:      14:33:34.000  1768401214.000  1452436432.000  p  c     s  f 
      ts1:      14:33:36.000  1768401216.000  1452436434.000  p  c  g  s    
      ts2:      14:33:38.000  1768401218.000  1452436436.000  p  c  g  s    
      ts3:      14:33:40.000  1768401220.000  1452436438.000  p  c  g  s    
      ts4:      14:33:42.000  1768401222.000  1452436440.000  p  c  g  s    
      ts5:      14:33:44.000  1768401224.000  1452436442.000  p  c  g  s    
      ts6:      14:33:46.000  1768401226.000  1452436444.000  p  c  g  s    
      ts7:      14:33:48.000  1768401228.000  1452436446.000  p  c  g  s    
      ts8:      14:33:50.000  1768401230.000  1452436448.000  p  c  g  s    
      ts9:      14:33:52.000  1768401232.000  1452436450.000  p  c  g  s    
     ts10:      14:33:54.000  1768401234.000  1452436452.000  p  c  g  s    
     ts11:      14:33:56.000  1768401236.000  1452436454.000  p  c  g  s    
     ts12:      14:33:58.000  1768401238.000  1452436456.000  p  c  g  s    
     ts13:      14:34:00.000  1768401240.000  1452436458.000  p  c  g  s    
     ts14:      14:34:02.000  1768401242.000  1452436460.000  p  c  g  s    
     ts15:      14:34:04.000  1768401244.000  1452436462.000  p  c  g  s    
     ts16:      14:34:06.000  1768401246.000  1452436464.000  p  c  g  s    
     ts17:      14:34:08.000  1768401248.000  1452436466.000  p  c  g  s    
     ts18:      14:34:10.000  1768401250.000  1452436468.000  p  c  g  s    
     ts19:      14:34:12.000  1768401252.000  1452436470.000  p  c  g  s    
     ts20:      14:34:14.000  1768401254.000  1452436472.000  p  c  g  s    
     ts21:      14:34:16.000  1768401256.000  1452436474.000  p  c  g  s    
     ts22:      14:34:18.000  1768401258.000  1452436476.000  p  c  g  s    
     ts23:      14:34:20.000  1768401260.000  1452436478.000  p  c  g  s    
     ts24:      14:34:22.000  1768401262.000  1452436480.000  p  c  g  s    
     ts25:      14:34:24.000  1768401264.000  1452436482.000  p  c  g  s    
     ts26:      14:34:26.000  1768401266.000  1452436484.000  p  c  g  s    
     ts27:      14:34:28.000  1768401268.000  1452436486.000  p  c  g  s    
     ts28:      14:34:30.000  1768401270.000  1452436488.000  p  c  g  s    
     ts29:      14:34:32.000  1768401272.000  1452436490.000  p  c  g  s    
     ts30:      14:34:34.000  1768401274.000  1452436492.000  p  c  g  s    
     ts31:      14:34:36.000  1768401276.000  1452436494.000  p  c  g  s    
     ts32:      14:34:38.000  1768401278.000  1452436496.000  p  c  g  s    
     ts33:      14:34:40.000  1768401280.000  1452436498.000  p  c  g  s    
     ts34:      14:34:42.000  1768401282.000  1452436500.000  p  c  g  s    
     ts35:      14:34:44.000  1768401284.000  1452436502.000  p  c  g  s    
     ts36:      14:34:46.000  1768401286.000  1452436504.000  p  c  g  s    
     ts37:      14:34:48.000  1768401288.000  1452436506.000  p  c  g  s    
     ts38:      14:34:50.000  1768401290.000  1452436508.000  p  c  g  s    
     ts39:      14:34:52.000  1768401292.000  1452436510.000  p  c  g  s    
     ts40:      14:34:54.000  1768401294.000  1452436512.000  p  c  g  s    
     ts41:      14:34:56.000  1768401296.000  1452436514.000  p  c  g  s    
     ts42:      14:34:58.000  1768401298.000  1452436516.000  p  c  g  s    
     ts43:      14:35:00.000  1768401300.000  1452436518.000  p  c  g  s    
     ts44:      14:35:02.000  1768401302.000  1452436520.000  p  c  g  s    
     ts45:      14:35:04.000  1768401304.000  1452436522.000  p  c  g  s    
     ts46:      14:35:06.000  1768401306.000  1452436524.000  p  c  g  s    
     ts47:      14:35:08.000  1768401308.000  1452436526.000  p  c  g  s    
     ts48:      14:35:10.000  1768401310.000  1452436528.000  p  c  g  s    
     ts49:      14:35:12.000  1768401312.000  1452436530.000  p  c  g  s    
     ts50:      14:35:14.000  1768401314.000  1452436532.000  p  c  g  s    
     ts51:      14:35:16.000  1768401316.000  1452436534.000  p  c  g  s    
     ts52:      14:35:18.000  1768401318.000  1452436536.000  p  c  g  s    
     ts53:      14:35:20.000  1768401320.000  1452436538.000  p  c  g  s    
     ts54:      14:35:22.000  1768401322.000  1452436540.000  p  c  g  s    
     ts55:      14:35:24.000  1768401324.000  1452436542.000  p  c  g  s    
     ts56:      14:35:26.000  1768401326.000  1452436544.000  p  c  g  s    
     ts57:      14:35:28.000  1768401328.000  1452436546.000  p  c  g  s    
     ts58:      14:35:30.000  1768401330.000  1452436548.000  p  c  g  s    
     ts59:      14:35:32.000  1768401332.000  1452436550.000  p  c  g  s    
    
    Coarse channel details (metafits=24, provided=24, common=24, good=24, select=24, flag=0, ranges=1):
            gpu  corr  rec  cen [MHz]  p  c  g  s  f  range 
      cc0:  131     0  131   167.6800  p  c  g  s     0 (0-23) 
      cc1:  132     1  132   168.9600  p  c  g  s     0 (0-23) 
      cc2:  133     2  133   170.2400  p  c  g  s     0 (0-23) 
      cc3:  134     3  134   171.5200  p  c  g  s     0 (0-23) 
      cc4:  135     4  135   172.8000  p  c  g  s     0 (0-23) 
      cc5:  136     5  136   174.0800  p  c  g  s     0 (0-23) 
      cc6:  137     6  137   175.3600  p  c  g  s     0 (0-23) 
      cc7:  138     7  138   176.6400  p  c  g  s     0 (0-23) 
      cc8:  139     8  139   177.9200  p  c  g  s     0 (0-23) 
      cc9:  140     9  140   179.2000  p  c  g  s     0 (0-23) 
     cc10:  141    10  141   180.4800  p  c  g  s     0 (0-23) 
     cc11:  142    11  142   181.7600  p  c  g  s     0 (0-23) 
     cc12:  143    12  143   183.0400  p  c  g  s     0 (0-23) 
     cc13:  144    13  144   184.3200  p  c  g  s     0 (0-23) 
     cc14:  145    14  145   185.6000  p  c  g  s     0 (0-23) 
     cc15:  146    15  146   186.8800  p  c  g  s     0 (0-23) 
     cc16:  147    16  147   188.1600  p  c  g  s     0 (0-23) 
     cc17:  148    17  148   189.4400  p  c  g  s     0 (0-23) 
     cc18:  149    18  149   190.7200  p  c  g  s     0 (0-23) 
     cc19:  150    19  150   192.0000  p  c  g  s     0 (0-23) 
     cc20:  151    20  151   193.2800  p  c  g  s     0 (0-23) 
     cc21:  152    21  152   194.5600  p  c  g  s     0 (0-23) 
     cc22:  153    22  153   195.8400  p  c  g  s     0 (0-23) 
     cc23:  154    23  154   197.1200  p  c  g  s     0 (0-23) 
    
    Antenna details (all=128, flag=1)
    Baseline Details (all=8256, auto=128, select=8256, flag=128):
    Estimated memory usage per timestep =              768ch *   8256bl * (32<Jones<f32>> + 4<f32> + 1<bool>) =    0.22 GiB
    Estimated memory selected           =    60ts *    768ch *   8256bl * (32<Jones<f32>> + 4<f32> + 1<bool>) =   13.11 GiB
    Estimated output size               =    60ts *    768ch *   8256bl * 4pol * (8<c32> + 4<f32> + 1<bool>) =   18.42 GiB
    Preprocessing Context: 
    Will not correct Van Vleck.
    Will correct cable lengths.
    Will correct digital gains.
    Will not correct coarse pfb passband gains.
    Will flag with aoflagger strategy /usr/local/share/aoflagger/strategies/mwa-default.lua
    Will correct geometry.
    
    
[2026-01-14T14:38:39Z INFO  birli] correct_digital duration: 642.522771ms
[2026-01-14T14:38:39Z INFO  birli] init duration: 873.682µs
[2026-01-14T14:38:39Z INFO  birli] flag duration: 10.513887787s
[2026-01-14T14:38:39Z INFO  birli] read duration: 629.156978ms
[2026-01-14T14:38:39Z INFO  birli] correct_geom duration: 235.346444ms
[2026-01-14T14:38:39Z INFO  birli] write duration: 31.131080051s
[2026-01-14T14:38:39Z INFO  birli] correct_cable duration: 873.392819ms
[2026-01-14T14:38:39Z INFO  birli] total duration: 44.026260532s
[2026-01-14T14:38:39Z INFO  birli] Estimated data read     =    60ts *    768ch *   8256bl * (32<Jones<f32>> + 4<f32> + 1<bool>) =   13.11 GiB @ 21336.486 MiB/s
[2026-01-14T14:38:39Z INFO  birli] Estimated data written  =    60ts *    768ch *   8256bl * 4pol * (8<c32> + 4<f32> + 1<bool>)  =   18.42 GiB @  606.018 MiB/s

