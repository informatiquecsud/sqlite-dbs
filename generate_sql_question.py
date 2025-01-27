import os
import sqlite3
import uuid

db = 'earthquakes.db'
db = 'imdb_small.db'
db = 'ecole.db'
db = 'films.db'
db = 'population_small.db'
db = 'chinook_tracks.db'
localpath = os.path.join(os.getcwd(), 'dbs')
path = 'https://raw.githubusercontent.com/informatiquecsud/sqlite-dbs/main/dbs/'
debug = False

max_assertions = 30
max_output_rows = 10
unittests = True
hide_solution = True
target = 'question' # one of ['demo', 'question']
mode = 'butreq' # one of ['butreq', 'activecode']
show_result = True

question_id = 'db_sql_2425_B_requete_1'
but_req_sql = '''
..  butreq::
    Déterminer les différentes "unités" utilisées dans la base de données en affichant toutes les différentes valeurs présentes dans la colonne `Unité`
..  sql::
    SELECT DISTINCT Unité
    FROM population
'''
question_id = 'db_sql_2425_B_requete_2'
but_req_sql = '''
..  butreq::
    Déterminez le nombre de communes du canton du Jura (utiliser la colonne `Canton`)
..  sql::
    SELECT COUNT(DISTINCT nom_commune)
    FROM population
    WHERE canton = 'JU' and Année = 2020;
'''


question_id = 'db_sql_2425_B_requete_3'
but_req_sql = '''
..  butreq::
    Déterminez le nom de la commune du Valais dont la population de nationalité suisse est la plus faible en 2020
..  sql::
    SELECT Nom_commune AS "Commune", Nombre as "Nombre de nationaux"
    FROM population
    WHERE canton = 'VS' and Unité = 'Nationalité - Suisse' AND Année = 2020
    ORDER BY Nombre ASC
    LIMIT 1
'''
question_id = 'db_sql_2425_B_requete_4'
but_req_sql = '''
..  butreq::
    Déterminez la population étrangère moyenne par commune en 2020 pour chacun des cantons. 
..  sql::
    SELECT ROUND(AVG(Nombre), 0) AS "Moyenne", Canton
    FROM population
    WHERE Année = 2020 AND unité = 'Nationalité - Etranger'
    GROUP BY Canton
    ORDER BY "Moyenne" DESC
'''
question_id = 'db_sql_2425_B_requete_0'
but_req_sql = '''
..  butreq::
    Écrivez une requête qui affiche les 10 premières lignes de la table ``population``
..  sql::
    SELECT * FROM population LIMIT 10;
'''
question_id = 'db_sql_2425_C_requete_2'
but_req_sql = '''
..  butreq::
    Déterminez les noms de tous les districts suisses présents dans la base de données.
..  sql::
    SELECT DISTINCT Nom_district
    FROM population
'''
question_id = 'db_sql_2425_C_requete_2'
but_req_sql = '''
..  butreq::
    Déterminez le nom de la commune du canton de Vaud dont la population de
    nationalité suisse est la plus faible en 2015. Faites en sorte que les
    noms des colonnes soient indiquées dans le résultat dans l'exemple
    (``Commune`` et ``Nombre de nationaux``)
..  sql::
    SELECT Nom_commune AS "Commune", Nombre as "Nombre de nationaux"
    FROM population
    WHERE canton = 'VD' and Unité = 'Nationalité - Suisse' AND Année = 2015
    ORDER BY Nombre ASC
    LIMIT 1
'''
question_id = 'db_sql_2425_C_requete_3'
but_req_sql = '''
..  butreq::
    Déterminez, pour chaque canton, la population étrangère de la commune
    ayant la plus grande population étrangère et celle de la commune ayant
    la plus faible population étrangère en 2015. Triez le résultat de manière décroissante
    en fonction de la colonne
    indiquant la population étrangère maximale.
..  sql::
    SELECT Nom_canton, MAX(Nombre), MIN(Nombre)
    FROM population
    WHERE Unité = 'Nationalité - Etranger' AND Année = 2015 
    GROUP BY Nom_canton
    ORDER BY MAX(Nombre) DESC
'''
question_id = 'db_sql_2425_D_requete_1'
but_req_sql = '''
..  butreq::
    Déterminez les noms de tous les compositeurs présents dans la table,
    triés par ordre alphabétique croissant.
..  sql::
    SELECT DISTINCT Genre
    FROM tracks
    ORDER BY Genre
'''
question_id = 'db_sql_2425_D_requete_2'
but_req_sql = '''
..  butreq::
    Déterminez le nombre de pistes se terminant par le mot 'Love' ou commençant par le mot 'life'
..  sql::
    SELECT COUNT(*) as "Nombre"
    FROM tracks
    WHERE name LIKE 'life%' OR name LIKE '%love'
'''
question_id = 'db_sql_2425_D_requete_3'
but_req_sql = '''
..  butreq::
    Déterminez les noms de tous les genres de musique possédant au moins une piste 
    dont la durée est située entre 300'000 ms et 600'000 ms. Pour chaque genre en question, 
    affichez le nombre de pistes de ce genre ainsi que la durée moyenne des pistes en question.
    Triez les résultats par ordre décroissant du nombre de pistes par genre.
..  sql::
    SELECT Genre, COUNT(*) as "Nombre de pistes", ROUND(AVG(Milliseconds), 0) as "Durée moyenne"
    FROM tracks
    WHERE Milliseconds > 300000 and Milliseconds < 600000
    GROUP BY Genre
    ORDER BY "Nombre de pistes" DESC
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
