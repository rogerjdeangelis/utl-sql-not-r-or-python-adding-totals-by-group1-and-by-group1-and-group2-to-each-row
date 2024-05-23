%let pgm=utl-sql-not-r-or-python-adding-totals-by-group1-and-by-group1-and-group2-to-each-row;

sql not r or python adding totals by group1 and by group1 and group2 to each row

    Solutions
         1 sas sql
         2 r sql
         3 python sql
         4 r native (not shown - could not get it to work)
         5 other sql solutions (should also work in python0 by
           https://stackoverflow.com/users/516548/g-grothendieck
           a partitioning
           b with
github
https://tinyurl.com/2y3krpw3
https://github.com/rogerjdeangelis/utl-sql-not-r-or-python-adding-totals-by-group1-and-by-group1-and-group2-to-each-row

RELATED

https://tinyurl.com/3cafbwmh
https://stackoverflow.com/questions/75418280/how-to-perform-sql-join-with-a-table-and-another-table-having-an-aggregate-value
https://stackoverflow.com/users/516548/g-grothendieck

https://goo.gl/WY1R7s
https://communities.sas.com/t5/Base-SAS-Programming/How-to-calculate-number-of-subjects-by-different-variablesin/m-p/326518

REPO
---------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-passing-arguments-to-sqldf-using-wps-python-f-text-function
https://github.com/rogerjdeangelis/utl-passing-arguments-to-sqldf-wps-r-sql-functional-sql

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                              |                                                    |                                    */
/*           INPUT              |            RULES PROCESS                           |         OUTPUT                     */
/*                              |                                                    |                                    */
/*  SAS SQL                     |                                                    |                                    */
/*                              |                                                    |                                    */
/* options validvarname=upcase; | SORTED FOR DOUMENTATION ONLY                       |     AGREES WITH THE RULES          */
/* libname sd1 "d:/sd1";        | ADD TOTAL COUNTS FOR and TRTxSEX to EACH ROW       |                                    */
/* data sd1.have;               |                                                    |                                    */
/*   input (ID TRT SEX) ($);    |                    TRT_FREQ    TRTXSEX             | ID    TRT   SEX TRT_FREQ TRT_SEX   */
/* cards4;                      | ID SEX TRT                                         |                                    */
/* 1 ASPIRIN M                  |  1  M  ASPIRIN   3 (ASPIRIN) 2 (MALE X ASPIRIN)    | 5   ASPIRIN  F      3       1      */
/* 2 PLACEBO M                  |  3  M  ASPIRIN   3 (ASPIRIN) 2 (MALE X ASPIRIN)    | 1   ASPIRIN  M      3       2      */
/* 3 ASPIRIN M                  |  5  F  ASPIRIN   3 (ASPIRIN) 1 (FEMALE X ASPIRIN)  | 3   ASPIRIN  M      3       2      */
/* 4 PLACEBO M                  |  6  F  OZEMPIC   1 (OZEMPIC) 1 (FEMALE X OZENPIC)  | 6   OZEMPIC  F      1       1      */
/* 5 ASPIRIN F                  |  2  M  PLACEBO   2 (PLACEBO) 2 (MALE X PLACEBO )   | 4   PLACEBO  M      2       2      */
/* 6 OZEMPIC F                  |  4  M  PLACEBO   2 (PLACEBO) 2 (MALE X PLACEBO )   | 2   PLACEBO  M      2       2      */
/* ;;;;                         |                                                    |                                    */
/* run;quit;                    |  NOTE THE TWO GROUP BYs (self documenting)         |                                    */
/*                              |                                                    |                                    */
/*  SD1.HAVE total obs=6        |   by trt                                           |                                    */
/*                              |   by trt, sex                                      |                                    */
/*  ID      TRT      SEX        |                                                    |                                    */
/*                              |  proc sql;                                         |                                    */
/*  1     ASPIRIN     M         |     create                                         |                                    */
/*  2     PLACEBO     M         |        table want as                               |                                    */
/*  3     ASPIRIN     M         |     select                                         |                                    */
/*  4     PLACEBO     M         |        *                                           |                                    */
/*  5     ASPIRIN     F         |       ,count(distinct id) as trt_sex               |                                    */
/*  6     OZEMPIC     F         |     from                                           |                                    */
/*                              |        (select                                     |                                    */
/*                              |            id                                      |                                    */
/*                              |           ,trt                                     |                                    */
/*                              |           ,sex                                     |                                    */
/*                              |           ,count(distinct id) as trt_freq          |                                    */
/*                              |         from                                       |                                    */
/*                              |            sd1.have                                |                                    */
/*                              |        group                                       |                                    */
/*                              |            by trt)                                 |                                    */
/*                              |     group                                          |                                    */
/*                              |          by trt ,sex                               |                                    */
/*                              |     order                                          |                                    */
/*                              |          by trt, sex, id                           |                                    */
/*                              |  ;quit;                                            |                                    */
/*                              |                                                    |                                    */
/*                              |                                                    |                                    */
/* R Python SQL                 | %utl_rbegin;                                       |                                    */
/*                              | parmcards4;                                        |                                    */
/*                              | library(haven)                                     |                                    */
/*                              | library(sqldf)                                     |                                    */
/*                              | have<-read_sas("d:/sd1/have.sas7bdat")             |                                    */
/*                              | have;                                              |                                    */
/*                              | want<- sqldf("                                     |                                    */
/*                              |  select                                            |                                    */
/*                              |    *                                               |                                    */
/*                              |   ,count(*) over (partition by trt) as trt_freq    |                                    */
/*                              |   ,count(*) over (partition by trt,sex) as trt_sex |                                    */
/*                              |  from                                              |                                    */
/*                              |      have")                                        |                                    */
/*                              | want                                               |                                    */
/*                              | ;;;;                                               |                                    */
/*                              | %utl_rend;                                         |                                    */
/*                              |                                                    |                                    */
/*                              |                                                    |                                    */
/*                              | R (not intuitive. Similar in Python?)              |                                    */
/*                              |                                                    |                                    */
/*                              | want<-cbind(have                                   |                                    */
/*                              |  %>% group_by(SEX, TRT)                            |                                    */
/*                              |  %>% mutate(TRT_FREQ = n()), have                  |                                    */
/*                              |  %>% group_by(TRT)                                 |                                    */
/*                              |  %>% mutate(TRT =                                  |                                    */
/*                              |   n()))[,.(ID,SEX,TRT,TRT_FREQ,TRT_SEX)];          |                                    */
/*                              |                                                    |                                    */
/**************************************************************************************************************************/

/*                   _           _ _             _       _   _
(_)_ __  _ __  _   _| |_    __ _| | |  ___  ___ | |_   _| |_(_) ___  ___
| | `_ \| `_ \| | | | __|  / _` | | | / __|/ _ \| | | | | __| |/ _ \/ __|
| | | | | |_) | |_| | |_  | (_| | | | \__ \ (_) | | |_| | |_| | (_) \__ \
|_|_| |_| .__/ \__,_|\__|  \__,_|_|_| |___/\___/|_|\__,_|\__|_|\___/|___/
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
  input (ID TRT SEX) ($);
cards4;
1 ASPIRIN M
2 PLACEBO M
3 ASPIRIN M
4 PLACEBO M
5 ASPIRIN F
6 OZEMPIC F
;;;;
run;quit;

/*                             _
/ |  ___  __ _ ___   ___  __ _| |
| | / __|/ _` / __| / __|/ _` | |
| | \__ \ (_| \__ \ \__ \ (_| | |
|_| |___/\__,_|___/ |___/\__, |_|
                            |_|
*/

 proc sql;
    create
       table want as
    select
       *
      ,count(distinct id) as trt_sex
    from
       (select
           id
          ,trt
          ,sex
          ,count(distinct id) as trt_freq
        from
           sd1.have
       group
           by trt)
    group
         by trt ,sex
    order
         by trt, sex, id
 ;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  WORK.WANT total obs=6                                                                                                 */
/*                                                                                                                        */
/*    ID      TRT      SEX    TRT_FREQ    TRT_SEX                                                                         */
/*                                                                                                                        */
/*    5     ASPIRIN     F         3          1                                                                            */
/*    1     ASPIRIN     M         3          2                                                                            */
/*    3     ASPIRIN     M         3          2                                                                            */
/*    6     OZEMPIC     F         1          1                                                                            */
/*    2     PLACEBO     M         2          2                                                                            */
/*    4     PLACEBO     M         2          2                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                     _
|___ \   _ __   ___  __ _| |
  __) | | `__| / __|/ _` | |
 / __/  | |    \__ \ (_| | |
|_____| |_|    |___/\__, |_|
                       |_|
*/

%utl_rbegin;
parmcards4;
library(haven)
library(sqldf)
 source("c:/temp/fn_tosas9.R")
have<-read_sas("d:/sd1/have.sas7bdat")
have;
want<- sqldf("
   select
       *
      ,count(*) over (partition by trt)     as trt_freq
      ,count(*) over (partition by trt,sex) as trt_sex
   from
       have")
want
fn_tosas9(dataf=want);
;;;;
%utl_rend;

libname tmp "c:/temp";
proc print data=tmp.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  R log                                                                                                                 */
/*                                                                                                                        */
/*    ID     TRT SEX trt_freq trt_sex                                                                                     */
/*  1  5 ASPIRIN   F        3       1                                                                                     */
/*  2  1 ASPIRIN   M        3       2                                                                                     */
/*  3  3 ASPIRIN   M        3       2                                                                                     */
/*  4  6 OZEMPIC   F        1       1                                                                                     */
/*  5  2 PLACEBO   M        2       2                                                                                     */
/*  6  4 PLACEBO   M        2       2                                                                                     */
/*                                                                                                                        */
/*  SAS                                                                                                                   */
/*                                                                                                                        */
/*  HAVE total obs=19                                                                                                     */
/*                                                                                                                        */
/*   ROWNAMES    ID      TRT      SEX    TRT_FREQ    TRT_SEX                                                              */
/*                                                                                                                        */
/*       1       5     ASPIRIN     F         3          1                                                                 */
/*       2       1     ASPIRIN     M         3          2                                                                 */
/*       3       3     ASPIRIN     M         3          2                                                                 */
/*       4       6     OZEMPIC     F         1          1                                                                 */
/*       5       2     PLACEBO     M         2          2                                                                 */
/*       6       4     PLACEBO     M         2          2                                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*____               _   _
|___ /   _ __  _   _| |_| |__   ___  _ __
  |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \
 ___) | | |_) | |_| | |_| | | | (_) | | | |
|____/  | .__/ \__, |\__|_| |_|\___/|_| |_|
        |_|    |___/
*/

%utl_pybegin;
parmcards4;
import os
import sys
import subprocess
import time
from os import path
import pandas as pd
import numpy as np
import pyreadstat as ps
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have,meta=ps.read_sas7bdat \
("d:/sd1/have.sas7bdat")
print(have)
want = pdsql('''
   select
       *
      ,count(*) over (partition by trt)     as trt_freq
      ,count(*) over (partition by trt,sex) as trt_sex
   from
       have
''')
print(want)
exec(open('c:/temp/fn_tosas9.py').read())
fn_tosas9(
   want
   ,dfstr="want"
   ,timeest=3
   )
;;;;
%utl_pyend;

libname tmp "c:/temp";
proc print data=tmp.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Python Log                                                                                                             */
/*                                                                                                                        */
/*    ID      TRT SEX  trt_freq  trt_sex                                                                                  */
/*  0  5  ASPIRIN   F         3        1                                                                                  */
/*  1  1  ASPIRIN   M         3        2                                                                                  */
/*  2  3  ASPIRIN   M         3        2                                                                                  */
/*  3  6  OZEMPIC   F         1        1                                                                                  */
/*  4  2  PLACEBO   M         2        2                                                                                  */
/*  5  4  PLACEBO   M         2        2                                                                                  */
/*                                                                                                                        */
/*  SAS                                                                                                                   */
/*                                                                                                                        */
/*    ID      TRT      SEX    TRT_FREQ    TRT_SEX                                                                         */
/*                                                                                                                        */
/*    5     ASPIRIN     F         3          1                                                                            */
/*    1     ASPIRIN     M         3          2                                                                            */
/*    3     ASPIRIN     M         3          2                                                                            */
/*    6     OZEMPIC     F         1          1                                                                            */
/*    2     PLACEBO     M         2          2                                                                            */
/*    4     PLACEBO     M         2          2                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                        _   _              __           _                       _   __
| || |    _ __   _ __   __ _| |_(_)_   _____   / / __   ___ | |_ __      _____  _ __| | _\ \
| || |_  | `__| | `_ \ / _` | __| \ \ / / _ \ | | `_ \ / _ \| __|\ \ /\ / / _ \| `__| |/ /| |
|__   _| | |    | | | | (_| | |_| |\ V /  __/ | | | | | (_) | |_  \ V  V / (_) | |  |   < | |
   |_|   |_|    |_| |_|\__,_|\__|_| \_/ \___| | |_| |_|\___/ \__|  \_/\_/ \___/|_|  |_|\_\| |
                                               \_\                                       /_/
*/

%utl_rbegin;
parmcards4;
library(haven)
library(dplyr)
library(data.table)
 source("c:/temp/fn_tosas9.R")
have<-read_sas("d:/sd1/have.sas7bdat")
have;
want<-cbind(have %>% group_by(SEX, TRT) %>% mutate(TRT_SEX = n()),
  have %>% group_by(TRT) %>% mutate(TRT_FREQ = n()))[,(ID,SEX,TRT_SEX,TRT_FREQ)];
;;;;
%utl_rend;
run;quit;

/*___          _   _                           _
| ___|    ___ | |_| |__   ___ _ __   ___  __ _| |   __ _ _ __  _____      _____ _ __ ___
|___ \   / _ \| __| `_ \ / _ \ `__| / __|/ _` | |  / _` | `_ \/ __\ \ /\ / / _ \ `__/ __|
 ___) | | (_) | |_| | | |  __/ |    \__ \ (_| | | | (_| | | | \__ \\ V  V /  __/ |  \__ \
|____/   \___/ \__|_| |_|\___|_|    |___/\__, |_|  \__,_|_| |_|___/ \_/\_/ \___|_|  |___/
                   _                        |_|
  __ _   _   _ ___(_)_ __   __ _
 / _` | | | | / __| | `_ \ / _` |
| (_| | | |_| \__ \ | | | | (_| |
 \__,_|  \__,_|___/_|_| |_|\__, |
                           |___/
*/

%utl_rbegin;
parmcards4;
library(sqldf)
DF<-read.table(header = TRUE, text = "
a b
1 1
1 2
1 3
2 4
2 5
2 6
")
want<-sqldf("select *, sum
  from DF A
  left join (select a, sum(b) as sum
    from DF
    group by a) using(a)")
want
;;;;
%utl_rend;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*   a b sum sum                                                                                                          */
/* 1 1 1   6   6                                                                                                          */
/* 2 1 2   6   6                                                                                                          */
/* 3 1 3   6   6                                                                                                          */
/* 4 2 4  15  15                                                                                                          */
/* 5 2 5  15  15                                                                                                          */
/* 6 2 6  15  15                                                                                                          */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                _ _   _
| |__   __      _(_) |_| |__
| `_ \  \ \ /\ / / | __| `_ \
| |_) |  \ V  V /| | |_| | | |
|_.__/    \_/\_/ |_|\__|_| |_|

*/


%utl_rbegin;
parmcards4;
library(sqldf)
DF<-read.table(header = TRUE, text = "
a b
1 1
1 2
1 3
2 4
2 5
2 6
")
want<-sqldf("with B as (
  select a, sum(b) as sum_with
    from DF
    group by a
)
select *
  from DF A
  left join B using(a)")
want;
;;;;
%utl_rend;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*    a b sum_with                                                                                                        */
/*  1 1 1        6                                                                                                        */
/*  2 1 2        6                                                                                                        */
/*  3 1 3        6                                                                                                        */
/*  4 2 4       15                                                                                                        */
/*  5 2 5       15                                                                                                        */
/*  6 2 6       15                                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
