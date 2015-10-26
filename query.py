import psycopg2

query_for_table = """
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_type = 'BASE TABLE' 
    	AND table_schema = 'public' 
		ORDER BY table_type, table_name;
"""
domain_table_finder = """
		SELECT cdu.TABLE_NAME
	FROM information_schema.domains AS d
	INNER JOIN information_schema.column_domain_usage AS cdu
	ON d.domain_name = cdu.domain_name
	WHERE d.domain_schema = 'public';

"""
query_for_check_constraint = """
	select
	ccu.column_name,

	tc.constraint_type

	FROM information_schema.constraint_column_usage ccu
	INNER JOIN information_schema.table_constraints tc

	ON ccu.constraint_name = tc.constraint_name
    AND tc.constraint_type = 'CHECK'

	where tc.table_name = '%s'

	;
"""
query_for_constraint = """
	select 
	kcu.column_name,
	
	tc.constraint_type
	
	FROM information_schema.key_column_usage kcu
	INNER JOIN information_schema.table_constraints tc

	ON kcu.constraint_name = tc.constraint_name
	
	where tc.table_name = '%s'
	ORDER BY constraint_type
	; 
"""
query_from_datatypes = """
	select 
	c.column_name,
	c.data_type,
	c.character_maximum_length,
	c.numeric_precision
	

	FROM information_schema.columns c

	WHERE c.table_name = '%s'
	
	;

	"""
query_for_unique_index="""
select


    a.attname as column_name
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and ix.indisunique = True
    and ix.indisprimary = False
    and t.relname = '%s'
order by
    t.relname,
    i.relname;


"""
query_for_check_domain = """
SELECT cc.check_clause

FROM information_schema.domain_constraints AS dc
INNER JOIN information_schema.check_constraints AS cc
ON cc.constraint_name = dc.constraint_name
INNER JOIN information_schema.column_domain_usage AS cdu
ON dc.domain_name = cdu.domain_name
WHERE cdu.TABLE_NAME = '%s'
;

"""
query_for_reference = """
				select 
				ccu.table_name , ccu.column_name
				from information_schema.constraint_column_usage ccu
				INNER JOIN information_schema.key_column_usage kcu
				ON ccu.constraint_name = kcu.constraint_name
				AND kcu.table_name='%s'
				AND kcu.column_name ='%s'
;

"""
query_for_domain_constraint = """
					SELECT cdu.column_name
 					FROM information_schema.column_domain_usage AS cdu
					WHERE cdu.table_name = '%s'
;


"""

query_for_check = """
        select

        cc.check_clause

        FROM information_schema.table_constraints tc
        INNER JOIN information_schema.check_constraints cc

        ON cc.constraint_name = tc.constraint_name
        INNER JOIN information_schema.constraint_column_usage ccu
        ON cc.constraint_name = ccu.constraint_name

        where tc.table_name = '%s'
        AND ccu.column_name = '%s'
        ;
"""
