import os
import sqlite3
import uuid

db = 'earthquakes.db'
db = 'imdb_small.db'
db = 'ecole.db'
localpath = os.path.join(os.getcwd(), 'dbs')
path = 'https://raw.githubusercontent.com/informatiquecsud/sqlite-dbs/main/dbs/'
debug = False

max_assertions = 10
max_output_rows = 10
unittests = True
hide_solution = True
target = 'question' # one of ['demo', 'question']
mode = 'butreq' # one of ['butreq', 'activecode']
show_result = True

question_id = ''
but_req_sql = '''
..  butreq::
    Compter le nombre de professeurs qui enseignent l'une des branches suivantes: ``Mathématiques``, ``Informatique``, ``Bureautique``
..  sql::
    SELECT COUNT(*) AS "Nombre de professeurs"
    FROM professeurs
    WHERE branche in ("Informatique", "Mathématiques", "Bureautique")
'''

def parse_runestone_activecode_sql(activecode):
    lines = activecode.strip().split('\n')
    
    id = ''
    dburl = ''
    
    state = 'read header'
    consigne = ''
    sql = ''
    
    nb_indent_spaces = 0
    indentation = ''
    
    
    for line in lines:
        if state == 'read header' and line.startswith('..  activecode::'):
            id = line.split('..  activecode:: ')[1]
            nb_indent_spaces = line.index('..  activecode')
            indentation = line[:nb_indent_spaces]
        elif state == 'read header' and line.strip().startswith(':dburl:'):
            dburl = line.strip().split(':dburl:')[1]
        elif state == 'read header' and line.strip() == '':
            state = 'read consigne'
        elif state == 'read consigne' and line.strip().startswith('~~~~'):
            state = 'read sql'
        
        elif state == 'read consigne':
            consigne += line[nb_indent_spaces + 4:] + '\n'
        elif state == 'read sql':
            sql += line[nb_indent_spaces + 4:]  + '\n'
                        
    return consigne, sql, id

def parse_butreq_sql(but_req_sql):
    lines = but_req_sql.split('\n')
    
    sql = but = ''

    state = 'none'
    for line in lines:
        if line.startswith('..  butreq'):
            state = 'read butreq'
        elif line.startswith('..  sql::'):
            state = 'read sql'
        
        elif state == 'none':
            ...
        elif state == 'read butreq':
            but += line[4:] + '\n'
        elif state == 'read sql':
            sql += line[4:] + '\n'
        else:
            ...
            
    return but, sql
        
if mode == 'butreq':
    but_req, sql = parse_butreq_sql(but_req_sql)
elif mode == 'activecode':
    but_req, sql, question_id = parse_runestone_activecode_sql(but_req_sql)

local_db_path = os.path.join(localpath, db)
runestone_db_path = f'{path}/{db}'


def get_correct_data(sql):
    if debug: print(f"path: {local_db_path}")
    connection = sqlite3.connect(local_db_path)
    #connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    try:
        if debug: print("sql executed:", sql)
        cursor.execute(sql)
    except sqlite3.OperationalError as e:
        print('Erreur dans la requête SQL : ' + str(e))

    description = cursor.description
    try:
        headers = [x[0] for x in description]
    except:
        headers = []

    rows = cursor.fetchall()
    
    if debug: print("rows", rows)

    # Il faut appliquer les changements à la base de données si on a fait
    # des requêtes INSERT, UPDATE ou DELETE qui affectent la base de données
    connection.commit()

    # Fermeture de la connexion à la base de données
    connection.close()

    return headers, rows


def xls_table(xls_pasted, sep='\t'):
    data = [line.split(sep) for line in xls_pasted.strip().split('\n')]
    return data


def rst_table_row(table_row):

    def first_col(data):
        return 4 * ' ' + f'* - {data}'

    def normal_col(data):
        return 4 * ' ' + f'  - {data}'

    line_rst = ''

    if len(table_row) == 0:
        raise ValueError('Rows cannot be empty')

    cols = [first_col(table_row[0])
            ] + [normal_col(col) for col in table_row[1:]]
    return cols


def rst_table(data,
              name='Data',
              headers=None,
              options=None,
              indent=0,
              nb_spaces=2):
    rst_lines = []
    options = options or {}
    indent_spaces = indent * nb_spaces * ' '
    EMPTY_LINE = ['']
    
    rows = data[:max_output_rows]
    
    if len(rows) == 0:
        headers = ['Message']
        rows = [["No result was produced"]]

    if headers:
        options['header-rows'] = 1
        
    if debug: print("data", rows)

    if 'widths' not in options:
        n = len(rows[0])
        options['widths'] = ' '.join([str(100 // n) for _ in range(n)])

    if 'align' not in options:
        options['align'] = 'left'

    if 'class' not in options:
        options['class'] = 'sql-data-table'

    rst_lines += [indent_spaces + f'..  list-table:: {name}']

    for opt, value in options.items():
        rst_lines += [indent_spaces + 4 * ' ' + f':{opt}: {value}']

    rst_lines += EMPTY_LINE

    if headers:
        rst_lines += [indent_spaces + line
                      for line in rst_table_row(headers)] + EMPTY_LINE

    for row in rows:
        rst_lines += [indent_spaces + line
                      for line in rst_table_row(row)] + EMPTY_LINE

    return '\n'.join(rst_lines)


def generate_runestone_sql_unittests(data):
    assertions = {}
    
    enum_date = list(enumerate(data))

    if len(data) > max_assertions:
        half = max_assertions // 2
        rows = enum_date[:half] + enum_date[-half:]
    else:
        rows = enum_date

    for row_num, row in rows:
        for col_num, col in enumerate(row):
            assertions[(row_num, col_num)] = col

    return '\n'.join(
        [f'assert {x},{y} == {data}' for (x, y), data in assertions.items()])
    
def prefix_each_line(multiline_string, prefix):
    return '\n'.join(
        [prefix + line for line in multiline_string.split('\n')])



def generate_question_rst(consigne,
                          sql,
                          id='',
                          hide_solution=False,
                          indent=0,
                          spaces_per_indent=4):


    one_indent = spaces_per_indent * ' '
    indent_spaces = indent * one_indent

    headers, rows = get_correct_data(sql)
    headers = [x for x in headers]
    
    table = rst_table(rows,
                      name='Résultat de la requête',
                      headers=headers,
                      indent=0)

    sql_code_block = f'..  code-block:: sql\n\n{prefix_each_line(sql, one_indent)}'
    id = id or str(uuid.uuid1())

    consigne = prefix_each_line(consigne, one_indent * 2)
    assertions = generate_runestone_sql_unittests(rows)
    assertions = prefix_each_line(assertions, one_indent)

    lines = [
        f'..  activecode:: {id}', 
        f'    :language: sql',
        f'    :dburl: {runestone_db_path}', 
        f'    :autograde: unittest' if unittests else '',
        '',
        f'    ..  admonition:: But de la requête',
        f'        :class: note',
        '',
        consigne, 
        f'    ..  reveal:: {id}-table',
        f'        :showtitle: Premières lignes du résultat attendu',
        '',
        prefix_each_line(table, one_indent * 2) if show_result else '', 
        '    ~~~~', 
        '', 
        '', 
        '    ====',
        '', 
        assertions if unittests else '', 
        '', 
        f'..  reveal:: {id}-solution',
        f'    :showtitle: Solution',
        f'    :instructoronly:' if hide_solution else '', 
        f'',
        f'{prefix_each_line(sql_code_block, one_indent)}'
    ]
    
    question_rst = [indent_spaces + line for line in lines]
    print('\n'.join(question_rst))
    
    
def generate_sql_demo(consigne,
                      sql,
                      id='',
                      hide_solution=False,
                      indent=0,
                      spaces_per_indent=4):

    one_indent = spaces_per_indent * ' '
    indent_spaces = indent * one_indent

    headers, rows = get_correct_data(sql)
    
    if debug: print("SQL", sql)
    
    table = rst_table(rows,
                      name='Résultat de la requête',
                      headers=headers,
                      indent=0)

    sql_code_block = f'{prefix_each_line(sql.strip(), one_indent)}'
    id = id or str(uuid.uuid1())

    consigne = prefix_each_line(consigne.strip(), one_indent * 2)

    lines = [
        f'..  activecode:: {id}', 
        f'    :language: sql',
        f'    :dburl: {runestone_db_path}', 
        '',
        f'    ..  admonition:: But de la requête',
        f'        :class: note',
        f'',
        consigne,
        '',
        prefix_each_line(table, one_indent) if show_result else '', 
        '',
        '    ~~~~', 
        '', 
        sql_code_block,
        ''
    ]

    question_rst = [indent_spaces + line for line in lines]
    print('\n'.join(question_rst))
    
    
if target == 'demo':
    what_to_generate = generate_sql_demo
elif target == 'question':
    what_to_generate = generate_question_rst
else:
    raise ValueError('target must be one of "demo" or "question"')
    

exo_rst = what_to_generate(but_req,
                           sql,
                           hide_solution=hide_solution,
                           id=question_id)
