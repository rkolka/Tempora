CREATE OR REPLACE PACKAGE TEMPORA
AS

FUNCTION RENAME_COL(
      p_tab_name in varchar2,
      P_COL_NAME IN VARCHAR2,
      p_old_col_name in varchar2)
   RETURN VARCHAR2 ;


FUNCTION ADD_COL$COLS(
      p_tab_name IN VARCHAR2 ,
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION CREATE_TAB$(
      P_TAB_NAME     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION CREATE_TAB$COL(
      P_TAB_NAME     IN VARCHAR2 ,
      P_COL_NAME     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2;
FUNCTION CREATE_TAB$COL_DEL_TRG(
      P_TAB_NAME     IN VARCHAR2 ,
      P_COL_NAME     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION CREATE_TAB$COL_SX(
      P_TAB_NAME IN VARCHAR2 ,
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION CREATE_TAB$COL_UPD_TRG(
      P_TAB_NAME     IN VARCHAR2 ,
      P_COL_NAME     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION CREATE_TAB$DEL_TRG(
      P_TAB_NAME     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION CREATE_TAB$INS_TRG(
      P_TAB_NAME     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION CREATE_TAB$PK(
      P_TAB_NAME     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION CREATE_TAB$PK_DEL_TRG(
      p_tab_name     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION CREATE_TAB$PK_UPD_TRG(
      p_tab_name     IN VARCHAR2 ,
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION CUR_CONS_COL_DEF(
      P_CONS_NAME IN VARCHAR2 )
   RETURN SPLIT_TBL PIPELINED ;

FUNCTION CUR_CONS_COL_NAME(
      P_CONS_NAME IN VARCHAR2 )
   RETURN SPLIT_TBL PIPELINED ;

FUNCTION CUR_DROP_TAB$(
      p_tab_name IN VARCHAR2 )
   RETURN split_tbl pipelined ;

FUNCTION DEF_COL(
      P_TAB_NAME IN VARCHAR2 ,
      P_COL_NAME IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION DEF_COL$(
      P_TAB_NAME IN VARCHAR2 ,
      P_COL_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION DEF_D$COL(
      p_default IN NUMBER := 1 )
   RETURN VARCHAR2 ;

FUNCTION DEF_U$COL(
      p_default IN NUMBER := 1 )
   RETURN VARCHAR2 ;

FUNCTION DML_INS_TAB$(
      p_tab_name     IN VARCHAR2 ,
      p_pk_cons_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION DROP_TAB$ALL(
      p_tab_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION EXPR_COMPOSE_FROM_CUR(
      p_cur_tab split_tbl ,
      p_regexp             IN VARCHAR2 ,
      p_separator VARCHAR2 := '' )
   RETURN VARCHAR2;

FUNCTION NAME_COL$(
      P_COL_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_D$$COL(
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_D$COL(
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_D$$PK(
      p_pk_cons_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_D$PK(
      p_pk_cons_name IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION NAME_TAB$(
      P_TAB_NAME IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION NAME_TAB$COL(
      P_TAB_NAME IN VARCHAR2 ,
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_TAB$COL_DEL(
      P_TAB_NAME IN VARCHAR2 ,
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION NAME_TAB$COL_SX(
      P_TAB_NAME IN VARCHAR2 ,
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_TAB$COL_UPD(
      P_TAB_NAME IN VARCHAR2 ,
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_TAB$DEL(
      P_TAB_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_TAB$INS(
      P_TAB_NAME IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION NAME_TAB$PK(
      P_TAB_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_TAB$PK_DEL(
      P_TAB_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_TAB$PK_UPD(
      P_TAB_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_U$$COL(
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_U$COL(
      p_col_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION NAME_U$$PK(
      p_pk_cons_name IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION NAME_U$PK(
      p_pk_cons_name IN VARCHAR2 )
   RETURN VARCHAR2;

FUNCTION PK_COL_DEF_CL(
      p_pk_cons_name IN VARCHAR2 )
   RETURN VARCHAR2 ;

FUNCTION PK_COL_NAME_CL(
      P_PK_CONS_NAME IN VARCHAR2 )
   RETURN VARCHAR2;

PROCEDURE TEMPORALIZE_TABLE(
      p_tab_name     IN VARCHAR2 ,
      p_pk_cons_name IN VARCHAR2 ,
      p_all          IN NUMBER := 0 ,
      p_execute      IN NUMBER := 0 ,
      p_insert       IN NUMBER := 0 );
END TEMPORA;
/


CREATE OR REPLACE PACKAGE body TEMPORA
AS


FUNCTION RENAME_COL(
      P_TAB_NAME IN VARCHAR2,
      P_COL_NAME IN VARCHAR2,
      P_OLD_COL_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result             VARCHAR2(32767) ;
   BEGIN
 
      -- Compose full DDL statement
      l_result :=
      ' 
-- rename the column itself
alter table <tab_name> rename column <old_col_name> to <col_name>;

-- its since data columns
alter table <tab$> rename column <name_d$old_col> to <name_d$col>;
alter table <tab$> rename column <name_u$old_col> to <name_u$col>;

-- its during data table and columns
alter table <name_tab$old_col> rename to <name_tab$col>;
alter table <name_tab$col> rename column <name_old_col$> to <name_col$>;
alter table <name_tab$col> rename column <name_d$old_col> to <name_d$col>;
alter table <name_tab$col> rename column <name_d$$old_col> to <name_d$$col>;
alter table <name_tab$col> rename column <name_u$old_col> to <name_u$col>;
alter table <name_tab$col> rename column <name_u$$old_col> to <name_u$$col>;
alter table <name_tab$col> rename constraint <name_tab$old_col>_pk to <name_tab$col>_pk;

-- create new triggers
-- column update trigger
<create_tab$col_upd_trg>

-- column delete trigger
<create_tab$col_del_trg>

-- drop old triggers
drop trigger <name_tab$old_col_upd>;
drop trigger <name_tab$old_col_upd>;
/
'
      ;

	  
-- column itself
      l_result := replace(l_result, '<table_name>', p_tab_name) ;
      l_result := replace(l_result, '<old_column_name>', p_old_col_name) ;
      l_result := replace(l_result, '<column_name>', p_col_name) ;

-- its since data columns
      l_result := replace(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := replace(l_result, '<name_d$old_col>', name_d$col(p_old_col_name)) ;
      l_result := replace(l_result, '<name_u$old_col>', name_u$col(p_old_col_name)) ;
      l_result := replace(l_result, '<name_d$col>', name_d$col(p_col_name)) ;
      l_result := replace(l_result, '<name_u$col>', name_u$col(p_col_name)) ;

-- its during data table and columns
      l_result := replace(l_result, '<name_tab$old_col>', name_tab$col(p_tab_name, p_old_col_name)) ;
      l_result := replace(l_result, '<name_tab$col>', name_tab$col(p_tab_name, p_col_name)) ;
      l_result := replace(l_result, '<name_old_col$>', name_col$(p_old_col_name)) ;
      l_result := replace(l_result, '<name_col$>', name_col$(p_col_name)) ;

      l_result := replace(l_result, '<name_d$old_col>', name_d$col(p_old_col_name)) ;
      l_result := replace(l_result, '<name_d$col>', name_d$col(p_col_name)) ;
      l_result := replace(l_result, '<name_d$$old_col>', name_d$$col(p_old_col_name)) ;
      l_result := replace(l_result, '<name_d$$col>', name_d$$col(p_col_name)) ;
      l_result := replace(l_result, '<name_u$old_col>', name_u$col(p_old_col_name)) ;
      l_result := replace(l_result, '<name_u$col>', name_u$col(p_col_name)) ;
      l_result := replace(l_result, '<name_u$$old_col>', name_u$$col(p_old_col_name)) ;
      l_result := replace(l_result, '<name_u$$col>', name_u$$col(p_col_name)) ;
-- during table constraint
      l_result := replace(l_result, '<name_u$$col>', name_u$$col(p_col_name)) ;

-- recreate triggers
      l_result := replace(l_result, '<name_tab$old_col_upd>', name_tab$col_upd(p_tab_name, p_old_col_name));
      l_result := replace(l_result, '<create_tab$col_upd_trg>', create_tab$col_upd_trg( p_tab_name, p_col_name, name_tab$col(p_tab_name, p_col_name) || '_pk')) ;

      l_result := replace(l_result, '<name_tab$old_col_del>', name_tab$col_del(p_tab_name, p_old_col_name)) ;
      l_result := replace(l_result, '<create_tab$col_del_trg>', create_tab$col_del_trg( p_tab_name, p_col_name, name_tab$col(p_tab_name, p_col_name) || '_pk')) ;

      RETURN l_result;
   end;
end rename_col;

   --------------------------------------------------------
   --  DDL for Function ADD_COL$COLS
   --------------------------------------------------------
FUNCTION ADD_COL$COLS(
      p_tab_name IN VARCHAR2,
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
IS
   l_result VARCHAR2(32767) ;
BEGIN
   l_result := 'ALTER TABLE <tab$> ADD (<name_d$col> <def_d$col>, <name_u$col> <def_u$col>)' ;
   l_result := REPLACE(l_result, '<tab$>', NAME_TAB$(p_tab_name)) ;
   l_result := REPLACE(l_result, '<name_d$col>', name_d$col(p_col_name)) ;
   l_result := REPLACE(l_result, '<name_u$col>', name_u$col(p_col_name)) ;
   l_result := REPLACE(l_result, '<def_d$col>', def_d$col(1)) ;
   l_result := REPLACE(l_result, '<def_u$col>', def_u$col(1)) ;
   RETURN l_result;
END add_col$cols;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$
--------------------------------------------------------
FUNCTION CREATE_TAB$(
      P_TAB_NAME IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result           VARCHAR2(32767) ;
      l_pk_grp_since_inf VARCHAR2(32767) := '';
   BEGIN
      l_result := q'[ 
CREATE TABLE <tab$> (    
<pk_col_def_cl>     
-- pk column definitions  
, <name_d$pk> <def_d$col>  
, <name_u$pk> <def_u$col>  
-- since info column definitions for pk. loosely creation time  
, CONSTRAINT <tab$>_PK PRIMARY KEY (<pk_col_name_cl>) ENABLE   
-- pk suits as PK for this table  
, CONSTRAINT <tab$>_FK FOREIGN KEY (<pk_col_name_cl>) REFERENCES <table_name> (<pk_col_name_cl>) ON DELETE CASCADE DISABLE   
-- disabled FK to underlying table
)
/
COMMENT ON TABLE <tab$> IS 'Table <tab$> records logged times aka transaction times and user info for table <table_name>'
/   
]'
      ;
      -- Compose full DDL statement
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<pk_grp_since_inf>', l_pk_grp_since_inf) ;
      l_result := REPLACE(l_result, '<pk_col_def_cl>', PK_COL_DEF_CL( P_PK_CONS_NAME)) ;
      l_result := REPLACE(l_result, '<name_d$pk>', name_d$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<def_d$col>', def_d$col(1)) ;
      l_result := REPLACE(l_result, '<name_u$pk>', name_u$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<def_u$col>', def_u$col(1)) ;
      RETURN l_result;
   END;
END create_tab$;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$COL
--------------------------------------------------------
FUNCTION CREATE_TAB$COL(
      P_TAB_NAME IN VARCHAR2,
      P_COL_NAME IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result        VARCHAR2(32767) ;
      l_pk_cons       VARCHAR2(400) := '';
      l_col_data_type VARCHAR2(200) ;
   BEGIN
      -- Compose full DDL statement
      l_result :=
      ' 
CREATE TABLE <name_tab$col> (
<pk_col_def_cl>, 
<name_col$> <def_col$>, 
<name_d$col> <def_d$col>,
<name_d$$col> <def_d$$col>, 
<name_u$col> <def_u$col>, 
<name_u$$col> <def_u$$col>, 
CONSTRAINT <name_tab$col>_PK 
PRIMARY KEY (<pk_col_name_cl> , <name_d$col>)
)'
      ;
      l_result := REPLACE(l_result, '<name_tab$col>', name_tab$col(p_tab_name, p_col_name)) ;
      l_result := REPLACE(l_result, '<pk_col_def_cl>', pk_col_def_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<name_col$>', name_col$(p_col_name)) ;
      l_result := REPLACE(l_result, '<def_col$>', def_col$(p_tab_name, p_col_name)) ;
      l_result := REPLACE(l_result, '<name_d$col>', name_d$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<def_d$col>', def_d$col(0)) ;
      l_result := REPLACE(l_result, '<name_d$$col>', name_d$$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<def_d$$col>', def_d$col(1)) ;
      l_result := REPLACE(l_result, '<name_u$col>', name_u$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<def_u$col>', def_u$col(0)) ;
      l_result := REPLACE(l_result, '<name_u$$col>', name_u$$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<def_u$$col>', def_u$col(1)) ;
      RETURN l_result;
   END;
END create_tab$col;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$COL_DEL_TRG
--------------------------------------------------------
FUNCTION CREATE_TAB$COL_DEL_TRG(
      P_TAB_NAME IN VARCHAR2,
      P_COL_NAME IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result             VARCHAR2(32767) ;
      l_col_ins_exp        VARCHAR2(2000) := '';
      l_pk_join_exp        VARCHAR2(2000) := '';
      l_old_pk_col_name_cl VARCHAR2(2000) := '';
      l_col_data_type      VARCHAR2(200) ;
      l_col_is_pk          INTEGER;
      l_when               VARCHAR2(2000) := '';
   BEGIN
      -- determine if <col> is part of pk, if it is then special case
      SELECT
         COUNT( *) AS arv
      INTO
         l_col_is_pk
      FROM
         user_cons_columns
      WHERE
         constraint_name = p_pk_cons_name
      AND table_name = p_tab_name
      AND column_name = p_col_name ;
      l_when := 'WHEN ( old.<column_name> is not null )  -- only if not null';
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), ':old.\1', ', ')
      INTO
         l_old_pk_col_name_cl
      FROM
         dual ;
      -- <pk_join_exp> is like tab$.pk1 = :new.pk1 (AND tab$.pkN = :new.pkN)*
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '<tab$>.\1 = :old.\1', ' AND ')
      INTO
         l_pk_join_exp
      FROM
         dual ;
      -- Compose full DDL statement
      l_result :=
      ' 
create or replace trigger <name_tab$col_del>
before delete on <table_name> for each row
<when>
declare  
since_inf_tuple <tab$>%rowtype;
begin   
select * into since_inf_tuple from <tab$> where <pk_join_exp>;    
insert into <name_tab$col>       
(<pk_col_name_cl>, <name_col$>, <d$col>, <u$col>)    
values       
(<old_pk_col_name_cl>, :old.<column_name>, since_inf_tuple.<d$col>, since_inf_tuple.<u$col>)   
;
end;
/
'
      ;
      IF l_col_is_pk > 0 THEN
         l_result := 'ERROR IN CREATE_TAB$$COL_DEL_TRG: COLUMN <table_name>.<column_name> is part of key ' || p_pk_cons_name || '. Use CREATE_TAB$PK_DEL_TRG!';
      END IF;
      l_result := REPLACE(l_result, '<when>', l_when) ;
      l_result := REPLACE(l_result, '<name_col$>', name_col$(p_col_name)) ;
      l_result := REPLACE(l_result, '<d$col>', name_d$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<u$col>', name_u$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<name_tab$col>', name_tab$col(p_tab_name, p_col_name)) ;
      l_result := REPLACE(l_result, '<name_tab$col_del>', name_tab$col_del( p_tab_name, p_col_name)) ;
      l_result := REPLACE(l_result, '<column_name>', p_col_name) ;
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      l_result := REPLACE(l_result, '<pk_join_exp>', l_pk_join_exp) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<old_pk_col_name_cl>', l_old_pk_col_name_cl) ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      RETURN l_result;
   END;
END create_tab$col_del_trg;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$COL_SX
--------------------------------------------------------
FUNCTION CREATE_TAB$COL_SX(
      P_TAB_NAME IN VARCHAR2,
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(32767) := '';
   BEGIN
      BEGIN
         l_result :=
         'delete from USER_SDO_GEOM_METADATA where table_name = ''<name_tab$col>'';         
INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) values (         
''<name_tab$col>''         
, ''<name_col$>''         
, (select diminfo from USER_SDO_GEOM_METADATA where table_name=''<tab_name>'')         
, (select srid from USER_SDO_GEOM_METADATA where table_name=''<tab_name>''));         
drop index <name_tab$col_sx>;         
CREATE INDEX <name_tab$col_sx> ON <name_tab$col> (<name_col$>) INDEXTYPE IS MDSYS.SPATIAL_INDEX ;      
'
         ;
         l_result := REPLACE(l_result, '<tab_name>', p_tab_name) ;
         l_result := REPLACE(l_result, '<col_name>', p_col_name) ;
         l_result := REPLACE(l_result, '<name_tab$col>', name_tab$col( p_tab_name, p_col_name)) ;
         l_result := REPLACE(l_result, '<name_tab$col_sx>', name_tab$col_sx( p_tab_name, p_col_name)) ;
         l_result := REPLACE(l_result, '<name_col$>', name_col$(p_col_name)) ;
      END;
      RETURN l_result;
   END;
END create_tab$col_sx;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$COL_UPD_TRG
--------------------------------------------------------
FUNCTION CREATE_TAB$COL_UPD_TRG(
      P_TAB_NAME IN VARCHAR2,
      P_COL_NAME IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result             VARCHAR2(32767) ;
      l_pk_join_exp        VARCHAR2(2000) := '';
      l_old_pk_col_name_cl VARCHAR2(2000) := '';
      l_col_data_type      VARCHAR2(200) ;
      l_col_is_pk          INTEGER;
      l_when               VARCHAR2(2000) := '';
      l_if                 VARCHAR2(2000) := '';
      l_end_if             VARCHAR2(2000) := '';
   BEGIN
      -- determine if <col> is part of pk, if it is then special case
      SELECT
         COUNT( *) AS arv
      INTO
         l_col_is_pk
      FROM
         user_cons_columns
      WHERE
         constraint_name = p_pk_cons_name
      AND table_name = p_tab_name
      AND column_name = p_col_name ;
      -- deteremine datatype of <col>, if SDO_GEOMETRY then special case
      SELECT
         data_type
      INTO
         l_col_data_type
      FROM
         user_tab_cols
      WHERE
         table_name = p_tab_name
      AND column_name = p_col_name ;
      IF l_col_data_type = 'SDO_GEOMETRY' THEN
         BEGIN
            l_if := 'IF (SDO_GEOM.RELATE(:new.<column_name>, ''EQUAL'', :old.<column_name>, 0.00000001) = ''FALSE'') THEN BEGIN   -- only if sdo_geometry' ;
            l_end_if := 'end; end if; -- only if sdo_geometry ';
         END;
      ELSE
         l_when :=
         'WHEN (new.<column_name> <> old.<column_name> or (old.<column_name> is not null and new.<column_name> is null) or (old.<column_name> is null and new.<column_name> is not null) )  -- only if not sdo_geometry'
         ;
      END IF;
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), ':old.\1', ', ')
      INTO
         l_old_pk_col_name_cl
      FROM
         dual ;
      -- <pk_join_exp> is like tab$.pk1 = :new.pk1 (AND tab$.pkN = :new.pkN)*
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '<tab$>.\1 = :new.\1', ' AND ')
      INTO
         l_pk_join_exp
      FROM
         dual ;
      -- Compose full DDL statement
      l_result :=
      ' 
create or replace trigger <name_tab$col_upd>
after update of <column_name> on <table_name> for each row
follows <name_tab$pk_upd> 
<when>
declare  
since_inf_tuple <tab$>%rowtype;
begin   
<if>       
select * into since_inf_tuple from <tab$> where <pk_join_exp>;       
if :old.<column_name> is not null then          
insert into  <name_tab$col>             
(<pk_col_name_cl>, <name_col$>, <d$col>, <u$col>)          
values             
( <old_pk_col_name_cl>, :old.<column_name>, since_inf_tuple.<d$col>, since_inf_tuple.<u$col>)         
;      
end if;         

update <tab$>         
set             
<d$col> = default,            
<u$col> = default         
where <pk_join_exp>  -- :old or :new depending on l_col_is_pk         
;    
<end_if>     

end;
/
'
      ;
      IF l_col_is_pk > 0 THEN
         l_result := 'ERROR IN CREATE_TAB$COL_UPD_TRG: COLUMN <table_name>.<column_name> is part of key ' || p_pk_cons_name || '. Use CREATE_TAB$PK_UPD_TRG!';
      END IF;
      l_result := REPLACE(l_result, '<when>', l_when) ;
      l_result := REPLACE(l_result, '<name_col$>', NAME_COL$(p_col_name)) ;
      l_result := REPLACE(l_result, '<if>', l_if) ;
      l_result := REPLACE(l_result, '<end_if>', l_end_if) ;
      l_result := REPLACE(l_result, '<d$col>', name_d$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<u$col>', name_u$col(p_col_name)) ;
      l_result := REPLACE(l_result, '<pk_join_exp>', l_pk_join_exp) ;
      l_result := REPLACE(l_result, '<name_tab$col>', name_tab$col(p_tab_name, p_col_name)) ;
      l_result := REPLACE(l_result, '<name_tab$col_upd>', name_tab$col_upd( p_tab_name, p_col_name)) ;
      l_result := REPLACE(l_result, '<name_tab$pk_upd>', name_tab$pk_upd( p_tab_name)) ;
      l_result := REPLACE(l_result, '<column_name>', p_col_name) ;
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := REPLACE(l_result, '<old_pk_col_name_cl>', l_old_pk_col_name_cl) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      RETURN l_result;
   END;
END create_tab$col_upd_trg;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$DEL_TRG
--------------------------------------------------------
FUNCTION CREATE_TAB$DEL_TRG(
      P_TAB_NAME IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result      VARCHAR2(32767) ;
      l_pk_join_exp VARCHAR2(400) ;
   BEGIN
      -- Deletes from transaction time table rows without matching base table -
      -- after statement trigger
      -- <pk_join_exp> is like ts.pk1 = t.pk1 (and ts.pkN = t.pkN)*
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), 'ts.\1 = t.\1', ' AND ')
      INTO
         l_pk_join_exp
      FROM
         dual ;
      -- Compose full DDL statement
      l_result :=
      ' 
create or replace trigger <tab$del>
after delete on <table_name>  
begin 
delete from <tab$> ts
where not exists (select ''EXISTS'' from <table_name> t where <pk_join_exp>) ;   
end;   
'
      ;
      l_result := REPLACE(l_result, '<pk_join_exp>', l_pk_join_exp) ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := REPLACE(l_result, '<tab$del>', name_tab$del(p_tab_name)) ;
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      RETURN l_result;
   END;
END create_tab$del_trg;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$INS_TRG
--------------------------------------------------------
FUNCTION CREATE_TAB$INS_TRG(
      P_TAB_NAME IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result             VARCHAR2(32767) ;
      l_trg_name           VARCHAR2(200) ;
      l_new_pk_col_name_cl VARCHAR2(200) := '';
   BEGIN
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), ':new.\1', ' AND ')
      INTO
         l_new_pk_col_name_cl
      FROM
         dual ;
      -- Result: ':new.colname1[,:new.colname2]'
      -- These functoins in SQL because
      -- Error(2,10): PLS-00653: aggregate/table functions are not allowed in
      -- PL/SQL scope
      -- Compose full DDL statement
      l_result :=
      ' 
create or replace trigger <tab$ins> 
after insert on <table_name> for each row 
begin  
/* log since time for all attributes of table */
insert into <tab$> (<pk_col_name_cl>) 
values (<new_pk_col_name_cl>); 
end;
'
      ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := REPLACE(l_result, '<tab$ins>', name_tab$ins(p_tab_name)) ;
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<new_pk_col_name_cl>', l_new_pk_col_name_cl) ;
      RETURN l_result;
   END;
END create_tab$ins_trg;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$PK
--------------------------------------------------------
FUNCTION CREATE_TAB$PK(
      P_TAB_NAME IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result               VARCHAR2(32767) ;
      l_pk_cons              VARCHAR2(400) := '';
      l_col_data_type        VARCHAR2(200) ;
      l_pk_during_col_def_cl VARCHAR2(2000) := 'IF_YOU_SEE_THIS_THEN_IT_IS_AN_ERROR';
   BEGIN
      FOR v IN
      (
         SELECT
            column_value AS elem
         FROM
            TABLE(cur_cons_col_name(p_pk_cons_name))
      )
      LOOP
         IF l_pk_during_col_def_cl = 'IF_YOU_SEE_THIS_THEN_IT_IS_AN_ERROR' THEN
            l_pk_during_col_def_cl := NAME_COL$(v.elem) || ' ' || DEF_COL( P_TAB_NAME, v.elem) ;
         ELSE
            l_pk_during_col_def_cl := l_pk_during_col_def_cl || ', ' || chr(13) || NAME_COL$(v.elem) || ' ' || DEF_COL(P_TAB_NAME, v.elem) ;
         END IF;
      END LOOP;
      -- Compose full DDL statement
      l_result :=
      ' 
CREATE TABLE <name_tab$pk> (
<pk_col_def_cl>, 
<pk_during_col_def_cl>, 
<name_d$pk> <def_d$col>,
<name_d$$pk> <def_d$$col>, 
<name_u$pk> <def_u$col>, 
<name_u$$pk> <def_u$$col>, 
CONSTRAINT <name_tab$pk>_PK 
PRIMARY KEY (<pk_col_name_cl> , <name_d$pk>)
)'
      ;
      l_result := REPLACE(l_result, '<name_tab$pk>', name_tab$pk(p_tab_name)) ;
      l_result := REPLACE(l_result, '<pk_col_def_cl>', pk_col_def_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<name_d$pk>', name_d$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<def_d$col>', def_d$col(0)) ;
      l_result := REPLACE(l_result, '<name_d$$pk>', name_d$$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<def_d$$col>', def_d$col(1)) ;
      l_result := REPLACE(l_result, '<name_u$pk>', name_u$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<def_u$col>', def_u$col(0)) ;
      l_result := REPLACE(l_result, '<name_u$$pk>', name_u$$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<def_u$$col>', def_u$col(1)) ;
      l_result := REPLACE(l_result, '<pk_during_col_def_cl>', l_pk_during_col_def_cl) ;
      RETURN l_result;
   END;
END create_tab$pk;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$PK_DEL_TRG
--------------------------------------------------------
FUNCTION CREATE_TAB$PK_DEL_TRG(
      p_tab_name IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result                VARCHAR2(32767) ;
      l_new_pk_col_name_cl    VARCHAR2(2000) := '';
      l_old_pk_col_name_cl    VARCHAR2(2000) := '';
      l_pk_join_exp           VARCHAR2(2000) := '';
      l_pk_upd_exp            VARCHAR2(2000) := '';
      l_col_is_pk             INTEGER;
      l_pk_during_col_name_cl VARCHAR2(2000) := '';
      l_when                  VARCHAR2(2000) := '';
   BEGIN
      -- finding correspondig row from tab$ needs old pk.
      -- <pk_join_exp> is like tab$.pk1 = :old.pk1 (AND tab$.pkN = :old.pkN)*
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '<tab$>.\1 = :old.\1', ' AND ')
      INTO
         l_pk_join_exp
      FROM
         dual ;
      -- we need to update tab$'s pk.
      -- <l_pk_upd_exp> is like tab$.pk1 = :new.pk1 (, tab$.pkN = :new.pkN)*
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '\1 = :new.\1', ', ')
      INTO
         l_pk_upd_exp
      FROM
         dual ;
      -- we need to compare tab$'s pk.
      -- <when> is like new.pk1 <> old.pk1 (OR new.pkN <> old.pkN)*
      -- observe difference with non-pk column, which possobly can be NULL
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), 'new.\1 <> old.\1', ' OR ')
      INTO
         l_when
      FROM
         dual ;
      -- <new_pk_col_name_cl> is like new.pk1 <> old.pk1 (OR new.pkN <> old.pkN
      -- )*
      --
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), ':new.\1', ', ')
      INTO
         l_new_pk_col_name_cl
      FROM
         dual ;
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), ':old.\1', ', ')
      INTO
         l_old_pk_col_name_cl
      FROM
         dual ;
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '\1$', ', ')
      INTO
         l_pk_during_col_name_cl
      FROM
         dual ;
      -- Compose full DDL statement
      l_result :=
      ' 
create or replace trigger <name_tab$pk_del>
before delete on <table_name> for each row

declare  
since_inf_tuple <tab$>%rowtype;
begin   
select * into since_inf_tuple from <tab$> where <pk_join_exp>;      

insert into <name_tab$pk>    
(<pk_col_name_cl>, <pk_during_col_name_cl>,    
<d$pk>, <u$pk>)  
values (    
<old_pk_col_name_cl>,    
<old_pk_col_name_cl>,    
since_inf_tuple.<d$pk>,    
since_inf_tuple.<u$pk>    
)  
;   

end;
'
      ;
      l_result := REPLACE(l_result, '<when>', l_when) ;
      l_result := REPLACE(l_result, '<d$pk>', name_d$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<u$pk>', name_u$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<pk_upd_exp>', l_pk_upd_exp) ;
      l_result := REPLACE(l_result, '<pk_join_exp>', l_pk_join_exp) ;
      l_result := REPLACE(l_result, '<name_tab$pk>', name_tab$pk(p_tab_name)) ;
      l_result := REPLACE(l_result, '<name_tab$pk_del>', name_tab$pk_del( p_tab_name)) ;
      l_result := REPLACE(l_result, '<column_name>', p_pk_cons_name) ;
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<new_pk_col_name_cl>', l_new_pk_col_name_cl) ;
      l_result := REPLACE(l_result, '<old_pk_col_name_cl>', l_old_pk_col_name_cl) ;
      l_result := REPLACE(l_result, '<pk_during_col_name_cl>', l_pk_during_col_name_cl) ;
      RETURN l_result;
   END;
END create_tab$pk_del_trg;
--------------------------------------------------------
--  DDL for Function CREATE_TAB$PK_UPD_TRG
--------------------------------------------------------
FUNCTION CREATE_TAB$PK_UPD_TRG(
      p_tab_name IN VARCHAR2,
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result                VARCHAR2(32767) ;
      l_new_pk_col_name_cl    VARCHAR2(2000) := '';
      l_old_pk_col_name_cl    VARCHAR2(2000) := '';
      l_pk_join_exp           VARCHAR2(2000) := '';
      l_pk_upd_exp            VARCHAR2(2000) := '';
      l_col_is_pk             INTEGER;
      l_pk_during_col_name_cl VARCHAR2(2000) := '';
      l_when                  VARCHAR2(2000) := '';
   BEGIN
      -- finding correspondig row from tab$ needs old pk.
      -- <pk_join_exp> is like tab$.pk1 = :old.pk1 (AND tab$.pkN = :old.pkN)*
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '<tab$>.\1 = :old.\1', ' AND ')
      INTO
         l_pk_join_exp
      FROM
         dual ;
      -- we need to update tab$'s pk.
      -- <l_pk_upd_exp> is like tab$.pk1 = :new.pk1 (, tab$.pkN = :new.pkN)*
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '\1 = :new.\1', ', ')
      INTO
         l_pk_upd_exp
      FROM
         dual ;
      -- we need to compare tab$'s pk.
      -- <when> is like new.pk1 <> old.pk1 (OR new.pkN <> old.pkN)*
      -- observe difference with non-pk column, which possobly can be NULL
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), 'new.\1 <> old.\1', ' OR ')
      INTO
         l_when
      FROM
         dual ;
      -- <new_pk_col_name_cl> is like new.pk1 <> old.pk1 (OR new.pkN <> old.pkN
      -- )*
      --
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), ':new.\1', ', ')
      INTO
         l_new_pk_col_name_cl
      FROM
         dual ;
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), ':old.\1', ', ')
      INTO
         l_old_pk_col_name_cl
      FROM
         dual ;
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '\1$', ', ')
      INTO
         l_pk_during_col_name_cl
      FROM
         dual ;
      -- Compose full DDL statement
      l_result :=
      ' 
create or replace trigger <name_tab$pk_upd>
after update of <pk_col_name_cl> on <table_name> for each row
WHEN (<when>)
declare  
since_inf_tuple <tab$>%rowtype;
begin   
select * into since_inf_tuple from <tab$> where <pk_join_exp>;      

insert into <name_tab$pk>    
(<pk_col_name_cl>, <pk_during_col_name_cl>,    
<d$pk>, <u$pk>)  
values (    
<new_pk_col_name_cl>,    
<old_pk_col_name_cl>,    
since_inf_tuple.<d$pk>,    
since_inf_tuple.<u$pk>    
)  
;        

update <tab$>   
set     
<pk_upd_exp>,    
<d$pk> = default,    
<u$pk> = default  
where     
<pk_join_exp>   -- :old for pk columns   
;  
end;
'
      ;
      l_result := REPLACE(l_result, '<when>', l_when) ;
      l_result := REPLACE(l_result, '<d$pk>', name_d$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<u$pk>', name_u$pk(p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<pk_upd_exp>', l_pk_upd_exp) ;
      l_result := REPLACE(l_result, '<pk_join_exp>', l_pk_join_exp) ;
      l_result := REPLACE(l_result, '<name_tab$pk>', name_tab$pk(p_tab_name)) ;
      -- TODO! for pk it should be different
      l_result := REPLACE(l_result, '<name_tab$pk_upd>', name_tab$pk_upd( p_tab_name)) ;
      l_result := REPLACE(l_result, '<column_name>', p_pk_cons_name) ;
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', pk_col_name_cl( p_pk_cons_name)) ;
      l_result := REPLACE(l_result, '<new_pk_col_name_cl>', l_new_pk_col_name_cl) ;
      l_result := REPLACE(l_result, '<old_pk_col_name_cl>', l_old_pk_col_name_cl) ;
      l_result := REPLACE(l_result, '<pk_during_col_name_cl>', l_pk_during_col_name_cl) ;
      RETURN l_result;
   END;
END create_tab$pk_upd_trg;
--------------------------------------------------------
--  DDL for Function CUR_CONS_COL_DEF
--------------------------------------------------------
FUNCTION CUR_CONS_COL_DEF(
      P_CONS_NAME IN VARCHAR2)
   RETURN SPLIT_TBL PIPELINED
IS
BEGIN
   -- output table of column names referenced in constraint named P_CONS_NAME
   FOR col IN
   (
      SELECT
         column_name
         || ' '
         || def_col(table_name, column_name) AS def
      FROM
         USER_CONS_COLUMNS
      WHERE
         constraint_name = p_cons_name
   )
   LOOP
      pipe row(col.def) ;
   END LOOP;
   RETURN;
END CUR_CONS_COL_DEF;
--------------------------------------------------------
--  DDL for Function CUR_CONS_COL_NAME
--------------------------------------------------------
FUNCTION CUR_CONS_COL_NAME(
      P_CONS_NAME IN VARCHAR2)
   RETURN SPLIT_TBL PIPELINED
IS
BEGIN
   -- output table of column names referenced in constraint named P_CONS_NAME
   FOR col IN
   (
      SELECT
         column_name
      FROM
         USER_CONS_COLUMNS
      WHERE
         constraint_name = p_cons_name
   )
   LOOP
      pipe row(col.column_name) ;
   END LOOP;
   RETURN;
END CUR_CONS_COL_NAME;
--------------------------------------------------------
--  DDL for Function CUR_DROP_TAB$
--------------------------------------------------------
FUNCTION CUR_DROP_TAB$(
      p_tab_name IN VARCHAR2)
   RETURN split_tbl pipelined
IS
BEGIN
   --
   FOR col IN
   (
      SELECT
         ('drop '
         || object_type
         || ' '
         || object_name
         || '') AS drp
      FROM
         user_objects
      WHERE
         object_name LIKE '%'
         || p_tab_name
         || '%'
      and object_name LIKE '%$%'
      AND object_type NOT IN('INDEX')
   )
   LOOP
      pipe row(col.drp) ;
   END LOOP;
   RETURN;
END cur_drop_tab$;
--------------------------------------------------------
--  DDL for Function DEF_COL
--------------------------------------------------------
FUNCTION DEF_COL(
      P_TAB_NAME IN VARCHAR2,
      P_COL_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(32767) ;
   BEGIN
      SELECT
         data_type
         || DECODE(data_precision, NULL, '', '('
         || TO_CHAR(data_precision)
         || ','
         || TO_CHAR(data_scale)
         || ')')
         || DECODE(char_length, 0, '', '('
         || TO_CHAR(char_length)
         || ' '
         || DECODE(char_used, 'B', 'BYTE', 'C', 'CHAR')
         || ')')
         || ' '
         || DECODE(nullable, 'N', 'NOT NULL', '')
      INTO
         l_result
      FROM
         user_tab_cols
      WHERE
         table_name = p_tab_name
      AND column_name = p_col_name;
      RETURN l_result;
   END;
END def_col;
--------------------------------------------------------
--  DDL for Function DEF_COL$
--------------------------------------------------------
FUNCTION DEF_COL$(
      P_TAB_NAME IN VARCHAR2,
      P_COL_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(32767) ;
   BEGIN
      SELECT
         data_type
         || DECODE(data_precision, NULL, '', '('
         || TO_CHAR(data_precision)
         || ','
         || TO_CHAR(data_scale)
         || ')')
         || DECODE(char_length, 0, '', '('
         || TO_CHAR(char_length)
         || ' '
         || DECODE(char_used, 'B', 'BYTE', 'C', 'CHAR')
         || ')')
         || ' '
         || DECODE(nullable, 'N', 'NOT NULL', '')
      INTO
         l_result
      FROM
         user_tab_cols
      WHERE
         table_name = p_tab_name
      AND column_name = p_col_name;
      RETURN l_result;
   END;
END def_col$;
--------------------------------------------------------
--  DDL for Function DEF_D$COL
--------------------------------------------------------
FUNCTION DEF_D$COL(
      p_default IN NUMBER := 1)
   RETURN VARCHAR2
IS
   l_result VARCHAR2(32767) ;
BEGIN
   IF p_default = 1 THEN
      l_result := 'TIMESTAMP (0) DEFAULT sysdate NOT NULL';
   ELSE
      l_result := 'TIMESTAMP (0) NOT NULL';
   END IF;
   RETURN l_result;
END def_d$col;
--------------------------------------------------------
--  DDL for Function DEF_U$COL
--------------------------------------------------------
FUNCTION DEF_U$COL(
      p_default IN NUMBER := 1)
   RETURN VARCHAR2
IS
   l_result VARCHAR2(32767) ;
BEGIN
   IF p_default = 1 THEN
      l_result := 'VARCHAR2(20 CHAR) DEFAULT SYS_CONTEXT (''USERENV'', ''OS_USER'') NOT NULL' ;
   ELSE
      l_result := 'VARCHAR2(20 CHAR) NOT NULL';
   END IF;
   RETURN l_result;
END def_u$col;
--------------------------------------------------------
--  DDL for Function DML_INS_TAB$
--------------------------------------------------------
FUNCTION DML_INS_TAB$(
      p_tab_name IN VARCHAR2,
      p_pk_cons_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result         VARCHAR2(32767) ;
      l_pk_col_name_cl VARCHAR2(200) := '';
   BEGIN
      -- Result: 'colname1[, colname2]'
      SELECT
         expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '\1', ', ')
      INTO
         l_pk_col_name_cl
      FROM
         dual ;
      -- Compose full DML statement
      l_result := ' 
insert into <tab$> (<pk_col_name_cl>) (select <pk_col_name_cl> from <table_name>)
' ;
      l_result := REPLACE(l_result, '<tab$>', name_tab$(p_tab_name)) ;
      l_result := REPLACE(l_result, '<table_name>', p_tab_name) ;
      l_result := REPLACE(l_result, '<pk_col_name_cl>', l_pk_col_name_cl) ;
      RETURN l_result;
   END;
END dml_ins_tab$;
--------------------------------------------------------
--  DDL for Function DROP_TAB$ALL
--------------------------------------------------------
FUNCTION DROP_TAB$ALL(
      p_tab_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(32767) ;
   BEGIN
      -- compose pk column definition commalist
      SELECT
         chr(13)
         || expr_compose_from_cur(cur_drop_tab$(p_tab_name), '\1', ';'
         || chr(13))
         || chr(13)
      INTO
         l_result
      FROM
         dual ;
      RETURN l_result;
   END;
END drop_tab$all;
--------------------------------------------------------
--  DDL for Function EXPR_COMPOSE_FROM_CUR
--------------------------------------------------------
FUNCTION EXPR_COMPOSE_FROM_CUR(
      p_cur_tab split_tbl,
      p_regexp IN VARCHAR2,
      p_separator VARCHAR2 := '')
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(32767) := 'IF_YOU_SEE_THIS_THEN_IT_IS_AN_ERROR';
   BEGIN
      FOR v IN
      (
         SELECT
            column_value elem
         FROM
            TABLE(p_cur_tab)
      )
      LOOP
         IF l_result = 'IF_YOU_SEE_THIS_THEN_IT_IS_AN_ERROR' THEN
            l_result := regexp_replace(v.elem, '(^.*$)', p_regexp) ;
         ELSE
            l_result := l_result || p_separator || regexp_replace(v.elem, '(^.*$)', p_regexp) ;
         END IF;
      END LOOP;
      RETURN l_result;
   END;
END expr_compose_from_cur;
--------------------------------------------------------
--  DDL for Function NAME_COL$
--------------------------------------------------------
FUNCTION NAME_COL$(
      P_COL_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := P_COL_NAME || '$';
      RETURN l_result;
   END;
END name_col$;
--------------------------------------------------------
--  DDL for Function NAME_D$$COL
--------------------------------------------------------
FUNCTION NAME_D$$COL(
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'D$$' || p_col_name;
      RETURN l_result;
   END;
END name_d$$col;
--------------------------------------------------------
--  DDL for Function NAME_D$COL
--------------------------------------------------------
FUNCTION NAME_D$COL(
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'D$' || p_col_name;
      RETURN l_result;
   END;
END name_d$col;
--------------------------------------------------------
--  DDL for Function NAME_D$$PK
--------------------------------------------------------
FUNCTION NAME_D$$PK(
      p_pk_cons_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'D$$' || 'PK'; --TODO! if not complex pk use the single
      -- column name.
      RETURN l_result;
   END;
END name_d$$pk;
--------------------------------------------------------
--  DDL for Function NAME_D$PK
--------------------------------------------------------
FUNCTION NAME_D$PK(
      p_pk_cons_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'D$' || 'PK'; --TODO! if not complex pk use the single column
      -- name.
      RETURN l_result;
   END;
END name_d$pk;
--------------------------------------------------------
--  DDL for Function NAME_TAB$
--------------------------------------------------------
FUNCTION NAME_TAB$(
      P_TAB_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   begin
      l_result := 'X$' || p_tab_name ;
      RETURN l_result;
   END;
END name_tab$;
--------------------------------------------------------
--  DDL for Function NAME_TAB$COL
--------------------------------------------------------
FUNCTION NAME_TAB$COL(
      P_TAB_NAME IN VARCHAR2,
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   begin
      l_result := 'X$' || p_tab_name || '$' || p_col_name;
      RETURN l_result;
   END;
END name_tab$col;
--------------------------------------------------------
--  DDL for Function NAME_TAB$COL_DEL
--------------------------------------------------------
FUNCTION NAME_TAB$COL_DEL(
      P_TAB_NAME IN VARCHAR2,
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := p_tab_name || '$' || p_col_name || '_DEL';
      RETURN l_result;
   END;
END name_tab$col_del;
--------------------------------------------------------
--  DDL for Function NAME_TAB$COL_SX
--------------------------------------------------------
FUNCTION NAME_TAB$COL_SX(
      P_TAB_NAME IN VARCHAR2,
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := p_tab_name || '$' || p_col_name || '_SX';
      RETURN l_result;
   END;
END name_tab$col_sx;
--------------------------------------------------------
--  DDL for Function NAME_TAB$COL_UPD
--------------------------------------------------------
FUNCTION NAME_TAB$COL_UPD(
      P_TAB_NAME IN VARCHAR2,
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := p_tab_name || '$' || p_col_name || '_UPD';
      RETURN l_result;
   END;
END name_tab$col_upd;
--------------------------------------------------------
--  DDL for Function NAME_TAB$DEL
--------------------------------------------------------
FUNCTION NAME_TAB$DEL(
      P_TAB_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := p_tab_name || '$' || 'DEL';
      RETURN l_result;
   END;
END name_tab$del;
--------------------------------------------------------
--  DDL for Function NAME_TAB$INS
--------------------------------------------------------
FUNCTION NAME_TAB$INS(
      P_TAB_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := p_tab_name || '$' || 'INS';
      RETURN l_result;
   END;
END name_tab$ins;
--------------------------------------------------------
--  DDL for Function NAME_TAB$PK
--------------------------------------------------------
FUNCTION NAME_TAB$PK(
      P_TAB_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   begin
      l_result := 'X$' || p_tab_name || '$PK';
      RETURN l_result;
   END;
END name_tab$pk;
--------------------------------------------------------
--  DDL for Function NAME_TAB$PK_DEL
--------------------------------------------------------
FUNCTION NAME_TAB$PK_DEL(
      P_TAB_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := p_tab_name || '$' || 'PK' || '_DEL';
      RETURN l_result;
   END;
END name_tab$pk_del;
--------------------------------------------------------
--  DDL for Function NAME_TAB$PK_UPD
--------------------------------------------------------
FUNCTION NAME_TAB$PK_UPD(
      P_TAB_NAME IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := p_tab_name || '$PK_UPD';
      RETURN l_result;
   END;
END name_tab$pk_upd;
--------------------------------------------------------
--  DDL for Function NAME_U$$COL
--------------------------------------------------------
FUNCTION NAME_U$$COL(
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'U$$' || p_col_name;
      RETURN l_result;
   END;
END name_u$$col;
--------------------------------------------------------
--  DDL for Function NAME_U$COL
--------------------------------------------------------
FUNCTION NAME_U$COL(
      p_col_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'U$' || p_col_name;
      RETURN l_result;
   END;
END name_u$col;
--------------------------------------------------------
--  DDL for Function NAME_U$$PK
--------------------------------------------------------
FUNCTION NAME_U$$PK(
      p_pk_cons_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'U$$' || 'PK'; --TODO! if not complex pk use the single
      -- column name.
      RETURN l_result;
   END;
END name_u$$pk;
--------------------------------------------------------
--  DDL for Function NAME_U$PK
--------------------------------------------------------
FUNCTION NAME_U$PK(
      p_pk_cons_name IN VARCHAR2)
   RETURN VARCHAR2
AS
BEGIN
   DECLARE
      l_result VARCHAR2(40) := '';
   BEGIN
      l_result := 'U$' || 'PK'; --TODO! if not complex pk use the single column
      -- name.
      RETURN l_result;
   END;
END name_u$pk;
--------------------------------------------------------
--  DDL for Function PK_COL_DEF_CL
--------------------------------------------------------
FUNCTION PK_COL_DEF_CL(
      p_pk_cons_name IN VARCHAR2)
   RETURN VARCHAR2
IS
   l_pk_col_def_cl VARCHAR2(32767) ;
BEGIN
   -- compose pk column definition commalist
   SELECT
      expr_compose_from_cur(CUR_CONS_COL_DEF(p_pk_cons_name), '\1', ', ')
   INTO
      l_pk_col_def_cl
   FROM
      dual ;
   RETURN l_pk_col_def_cl;
END PK_COL_DEF_CL;
--------------------------------------------------------
--  DDL for Function PK_COL_NAME_CL
--------------------------------------------------------
FUNCTION PK_COL_NAME_CL(
      P_PK_CONS_NAME IN VARCHAR2)
   RETURN VARCHAR2
IS
   l_result VARCHAR2(32767) := '';
BEGIN
   SELECT
      expr_compose_from_cur(cur_cons_col_name(p_pk_cons_name), '\1', ', ')
   INTO
      l_result
   FROM
      dual ;
   RETURN l_result;
END pk_col_name_cl;
--------------------------------------------------------
--  DDL for Procedure TEMPORALIZE_TABLE
--------------------------------------------------------
PROCEDURE TEMPORALIZE_TABLE(
      p_tab_name IN VARCHAR2,
      p_pk_cons_name IN VARCHAR2,
      p_all IN NUMBER := 0,
      p_execute IN NUMBER := 0,
      p_insert IN NUMBER := 0)
IS
   l_sql           VARCHAR2(32767) ; -- SQL statement
   l_col_data_type VARCHAR2(30) ;
BEGIN
   -- CREATE TABLE <TAB>$ ...
   l_sql := create_tab$(p_tab_name, p_pk_cons_name) ;
   IF P_EXECUTE = 1 THEN
      EXECUTE immediate l_sql;
   ELSE
      dbms_output.put_line(l_sql) ;
      dbms_output.put_line('/') ;
   END IF;
   -- insert existing ID-s in SINCE table
   l_sql := dml_ins_tab$(p_tab_name, p_pk_cons_name) ;
   IF P_EXECUTE = 1 THEN
      EXECUTE immediate l_sql;
   ELSE
      dbms_output.put_line(l_sql) ;
      dbms_output.put_line('/') ;
   END IF;
   -- Create trigger insert into <TAB>$
   l_sql := create_tab$ins_trg(p_tab_name, p_pk_cons_name) ;
   IF P_EXECUTE = 1 THEN
      EXECUTE immediate l_sql;
   ELSE
      dbms_output.put_line(l_sql) ;
      dbms_output.put_line('/') ;
   END IF;
   -- Create trigger delete from <TAB>$
   l_sql := create_tab$del_trg(p_tab_name, p_pk_cons_name) ;
   IF P_EXECUTE = 1 THEN
      EXECUTE immediate l_sql;
   ELSE
      dbms_output.put_line(l_sql) ;
      dbms_output.put_line('/') ;
   END IF;
   -- Create table for historic pk values
   l_sql := create_tab$pk(p_tab_name, p_pk_cons_name) ;
   IF P_EXECUTE = 1 THEN
      EXECUTE immediate l_sql;
   ELSE
      dbms_output.put_line(l_sql) ;
      dbms_output.put_line('/') ;
   END IF;
   -- Create update trigger for pk values
   l_sql := create_tab$pk_upd_trg(p_tab_name, p_pk_cons_name) ;
   IF P_EXECUTE = 1 THEN
      EXECUTE immediate l_sql;
   ELSE
      dbms_output.put_line(l_sql) ;
      dbms_output.put_line('/') ;
   END IF;
   -- Create delete trigger for pk values
   l_sql := create_tab$pk_del_trg(p_tab_name, p_pk_cons_name) ;
   IF P_EXECUTE = 1 THEN
      EXECUTE immediate l_sql;
   ELSE
      dbms_output.put_line(l_sql) ;
      dbms_output.put_line('/') ;
   END IF;
   -- for every COL of <TAB>
   FOR col IN
   (
      SELECT DISTINCT
         column_name
      FROM
         USER_TAB_COLUMNS
         --           natural join
         --          U$TD_COLUMNS
      WHERE
         table_name = p_tab_name
      AND column_name NOT IN('VERSION', 'MUUTJA', 'MUUDETUD', 'LOOJA', 'LOODUD')
      AND column_name NOT IN
         (
            SELECT
               column_value
            FROM
               TABLE(CUR_CONS_COL_NAME(p_pk_cons_name))
         )
         --         and TD_SOOV in ('A', 'AB')
   )
   LOOP
      -- deteremine datatype of <col>, if SDO_GEOMETRY then special case
      SELECT
         data_type
      INTO
         l_col_data_type
      FROM
         user_tab_cols
      WHERE
         table_name = p_tab_name
      AND column_name = col.column_name ;
      -- add SINCE info columns for COL
      l_sql := add_col$cols(p_tab_name, col.column_name) ;
      IF P_EXECUTE = 1 THEN
         EXECUTE immediate l_sql;
      ELSE
         dbms_output.put_line(l_sql) ;
         dbms_output.put_line('/') ;
      END IF;
      -- create DURING table for COL
      l_sql := CREATE_TAB$COL(p_tab_name, col.column_name, p_pk_cons_name) ;
      IF P_EXECUTE = 1 THEN
         EXECUTE immediate l_sql;
      ELSE
         dbms_output.put_line(l_sql) ;
         dbms_output.put_line('/') ;
      END IF;
      -- SDO_GEOMETRY needs spatial index and metadata
      IF l_col_data_type = 'SDO_GEOMETRY' THEN
         BEGIN
            l_sql := CREATE_TAB$COL_SX(p_tab_name, col.column_name) ;
            IF P_EXECUTE = 1 THEN
               EXECUTE immediate l_sql;
            ELSE
               dbms_output.put_line(l_sql) ;
               dbms_output.put_line('/') ;
            END IF;
         END;
      END IF;
      -- for every grantee of <TAB>
      FOR u IN
      (
         SELECT DISTINCT
            grantee
         FROM
            user_tab_privs
         WHERE
            table_name = p_tab_name
         AND privilege IN('UPDATE', 'DELETE', 'INSERT')
      )
      LOOP
         -- GRANT SELECT ON <TAB>$<COL> TO grantees of <TAB>
         l_sql := 'GRANT SELECT ON ' || NAME_TAB$COL(p_tab_name, col.column_name) || ' TO ' || u.grantee;
         IF P_EXECUTE = 1 THEN
            EXECUTE immediate l_sql;
         ELSE
            dbms_output.put_line(l_sql) ;
            dbms_output.put_line('/') ;
         END IF;
      END LOOP;
      l_sql := CREATE_TAB$COL_UPD_TRG(p_tab_name, col.column_name, p_pk_cons_name) ;
      IF P_EXECUTE = 1 THEN
         EXECUTE immediate l_sql;
      ELSE
         dbms_output.put_line(l_sql) ;
         dbms_output.put_line('/') ;
      END IF;
      l_sql := CREATE_TAB$COL_DEL_TRG(p_tab_name, col.column_name, p_pk_cons_name) ;
      IF P_EXECUTE = 1 THEN
         EXECUTE immediate l_sql;
      ELSE
         dbms_output.put_line(l_sql) ;
         dbms_output.put_line('/') ;
      END IF;
   END LOOP;
   -- for every grantee of <TAB>
   FOR u IN
   (
      SELECT DISTINCT
         grantee
      FROM
         user_tab_privs
      WHERE
         table_name = p_tab_name
      AND privilege IN('UPDATE', 'DELETE', 'INSERT')
   )
   LOOP
      -- GRANT SELECT ON <TAB>$ TO grantees of <TAB>
      l_sql := 'GRANT SELECT ON ' || name_tab$(p_tab_name) || ' TO ' || u.grantee;
      IF P_EXECUTE = 1 THEN
         EXECUTE immediate l_sql;
      ELSE
         dbms_output.put_line(l_sql) ;
         dbms_output.put_line('/') ;
      END IF;
   END LOOP;
END TEMPORALIZE_TABLE;
END TEMPORA;
/
