import csv
import mysql.connector
import psycopg2

# for testing purposes only


def saveToDb(connection, dicValues):
    cursor = connection.cursor()
    for v in dicValues:
        cursor.execute(
            f'insert into tabela.tabela (uuid, fullname) values (\'{v["uuid"]}\',\'{v["fullname"]}\')')
    connection.commit()
    cursor.close()


def get_env_variables():
    d = {}
    with open(".env", "r") as f:
        for line in f:
            (k, v) = line.split("=")
            v = v.split("\n")[0]
            d[k] = v
    # return d['host'], d['user'], d['password'], d['port'], d['database'], d['table']
    return d


def get_connection():
    env = get_env_variables()
    return psycopg2.connect(
        host=env['host'],
        user=env['user'],
        password=env['password'],
        port=env['port'],
        database=env['database']
    )
# Carrega o primeiro resultado de uma busca por campo e valor


def loadFirstByField(connection, table, field, value):
    cursor = connection.cursor()
    cursor.execute(
        f'select column_name from information_schema.columns where table_schema = \'{table}\' and table_name=\'{table}\'')
    column_names = [row[0] for row in cursor]
    sql = f'Select * from tabela.{table} where tabela.{field} = \'{value}\''
    cursor.execute(sql)
    rows = cursor.fetchone()
    my_result = None
    if (rows):
        my_result = dict(zip(column_names, rows))
    return my_result


def readCSV():
    result = []
    with open('MOCK_DATA2.csv', newline='') as arq:
        spamreader = csv.reader(arq, delimiter=',', quotechar='"')
        for row in spamreader:
            lines = row[0].split(";")
            uuid = (lines[0])
            fullname = (lines[1])
            if (uuid):
                result.append({
                    "uuid": uuid,
                    "fullname": fullname
                })
            else:
                result.append({
                    "uuid": 'no value'
                })
    return result


def writeCSV(dict, exclude=[]):

    with open('resultado.csv', 'w', newline='') as csvfile:
        fieldnames = ['Resultado']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in dict:
            writer.writerow({'Resultado': f'{row["uuid"]}'})


def main():
    arquivo = readCSV()
    connection = get_connection()
    tabela = get_env_variables()['table']
    field = get_env_variables()['pk']
    # saveToDb(connection=connection, dicValues=arquivo)

    resultado = []
    for linha in arquivo:
        linheira = loadFirstByField(
            connection=connection, table=tabela, field=field, value=linha[field])
        if linheira == None:
            resultado.append(f'{linha["uuid"]} - {linheira}')
    for r in resultado:
        print(r)


if __name__ == '__main__':
    main()
