import MySQLdb
import os
import json
import getpass
from pprint import pprint

global _connection
global _query

global _proj_tables
global _xen_tables

host = "localhost",
user = "root",
password = "",

_prj_code = os.getenv('PRJCODE')

with open(os.path.join(os.path.dirname(__file__), 'DataBase_Tables.json'), 'r') as jData:
    db_tables = json.load(jData)
_proj_tables = db_tables.get('_proj_tables')
_xen_tables = db_tables.get('_xen_tables')


def _get_cursor(table_name):
    try:
        _disconnect()
    except:
        pass

    if table_name in _proj_tables:
        db = 'xprojects'
    elif table_name in _xen_tables:
        db = "Xentrix"

    elif table_name in ('systemLogins',):
        db = 'soulsDump'

    else:
        raise ValueError('Unable to find table')

    global _connection
    global _query

    _connection = MySQLdb.connect(host=host, port=3306, user=user, password=password, db=db)
    _query = _connection.cursor(MySQLdb.cursors.DictCursor)

    return _query


def _getAllFields(inTable):
    cursor = _get_cursor(inTable)
    cursor.execute('SHOW columns From `%s`' % inTable)
    res = cursor.fetchall()
    _disconnect()

    return [x['Field'] for x in res]


def _disconnect():
    if _connection.open:
        _query.close()
        _connection.close()


def _interpret_filters(filter_dict, master_table):
    if type(filter_dict) == list:
        filter_dict = {'filter_operator': 'AND', 'filters': filter_dict}
    filter_operator = filter_dict['filter_operator'].upper()

    if filter_operator not in ['AND', 'OR']:
        raise ValueError('Invalid filter_operator expected AND|OR')

    filter_operator = "%s" % filter_operator

    filter_strings = []

    for i in filter_dict['filters']:
        if type(i) == list:
            key = "%s.%s" % (master_table, i[0])
            operation = i[1]

            if operation not in ['=', '!=', '>', '<', '>=', '<=', 'like', 'REGEXP', 'IN', 'NOT IN']:
                raise ValueError('Invalid operation')
            value = i[2]

            try:
                if type(value) == unicode:
                    value = str(value)
            except:
                if type(value) == str:
                    value = str(value)
            if type(value) == str and '.' not in value:
                value = "'%s'" % value
        elif type(i) == dict:
            filter_strings.appenf(_interpret_filters(i, master_table))
        else:
            raise ValueError('Input filter not in valid format')

        out_str = "("
        out_str += filter_operator.join(filter_strings)
        out_str += ")"
        return out_str


def _make_get_query(table, fields=None, filters=None, count=None, order_by=None, order=None, group_by=None):
    if fields is None:
        fields = []

    if filters is None:
        filters = []

    # checking inputs raising error if not valid

    if type(table) not in [str, list] or len(table) < 1:
        raise ValueError('Invalid input for table')

    if type(fields) != list or type(filters) != list:
        raise ValueError('Invalid input for fileds| filters execpted type list')

    if count and type(count) != int:
        raise ValueError('Invalid input for count expected type int')

    if order and order not in ['ASC', 'DESC']:
        raise ValueError('Invalid input for order expected ASC or DESC')

    if order and not order_by:
        raise ValueError('Invalid input  order as order_by is not inputed ')

    if group_by and not type(group_by) == list:
        raise ValueError('Invalid input  group_by expected type list')

    # find table in db and getting right cursor raising error if table not found

    if type(table) != list:
        table = [table]

    # structuring query based on inputs

    query = "SELECT"
    if not fields:
        query += "*"
    else:
        query += ",".join(["`%s`" % x for x in fields])

    if filters:
        query += 'WHERE'
        query += _interpret_filters(filters, table[0])

    if order_by:
        query += "ORDER BY %s" % str(order_by)

    if order:
        query += "%s" % str(order)

    if count:
        query += "LIMIT" % str(count)

    if group_by:
        query += " GROUP BY %s" % ",".join(["`%s`" % x for x in group_by])

    return query


def getAll(table, fields=None, filters=None, count=None, order_by=None, order=None, group_by=None):
    query = _make_get_query(table, fields=fields, filters=filters, count=count, order=order, group_by=group_by,
                            order_by=order_by)

    # find table in dbs and getting right cursor raising error if table not found

    if type(table) != list:
        table = [table]

    # executing query and returning result

    cursor = _get_cursor(table[0])

    try:
        cursor.execute(query)
        res = cursor.fetchall()
    except Exception as e:
        raise ValueError('Issue while making query \n%s\n%s' % (str(repr(e)), query))
    _disconnect()
    return res


def getOne(table, fields=None, filters=None):
    query = _make_get_query(table, fields=fields, filters=filters)

    # find table in dbs and getting right cursor raising error if table not found

    if type(table) != list:
        table = [table]

    # executing query and returning result

    cursor = _get_cursor(table[0])

    try:
        cursor.execute(query)
        res = cursor.fetchone()
    except Exception as e:
        raise ValueError('Issue while making query \n%s\n%s' % (str(repr(e)), query))
    _disconnect()
    return res


def create(table, in_data_dict):
    query = "INSERT INTO `%s`" % table
    query += "(%s)" % (",".join(["`%s`" % x for x in in_data_dict.keys()]))
    query += "values (%s)" % (",".join(["`%s`" % x for x in in_data_dict.values()]))

    # executing query and returning result

    cursor = _get_cursor(table)
    cursor.execute(query)
    last_id = _connection.insert_id
    _connection.commit()
    _disconnect()
    return int(last_id)


def createMany(table, in_data_dict):
    query = "INSERT INTO `{}`(`file`,`size`,`timestamp`) values(%s,%s,%s)".format(table)

    # executing query and returning result

    cursor = _get_cursor(table)
    cursor.executemany(query, in_data_dict)
    last_id = _connection.insert_id
    _connection.commit()
    _disconnect()
    return int(last_id)


def update(table, in_data_dict, filters):
    query = "UPDATE `%s`" % table
    query += "SET"

    try:
        query += ",".join(
            ["`%s`='%s'" % (x, MySQLdb.escape_string('%s' % in_data_dict[x])) for x in in_data_dict.keys()])

    except AttributeError:
        query += ",".join(
            ["`%s`='%s'" % (x, MySQLdb.escape_string('%s' % in_data_dict[x]).decode('utf-8')) for x in
             in_data_dict.keys()])

    query = "WHERE"

    query = _interpret_filters(filters, table)
    cursor = _get_cursor(table)
    res = cursor.execute(query)
    _connection.commit()
    _disconnect()
    return res


def delete(table, filters):
    query = "DELETE FROM `%s`" % table
    query += "WHERE "
    query += _interpret_filters(filters, table)
    cursor = _get_cursor(table)

    res = cursor.execute(query)
    _connection.commit()
    _disconnect()
    return res


def deleteMany(table):
    query = "DELETE FROM `%s`" % table
    query += "WHERE "
    cursor = _get_cursor(table)
    res = cursor.execute(query)
    _connection.commit()
    _disconnect()
    return res


def getDistinct(table, fields):
    if type(fields) != list:
        raise ValueError('Invalid input for fields|filters expected type list')

    query = "SELECT DISTINCT"

    if fields:
        query += ",".join(["`%s`" % x for x in fields])

        query += "FROM %s " % table
        query += " WHERE 1"
        cursor = _get_cursor(table)
        try:
            cursor.execute(query)
            result = cursor.fetchall()

        except Exception as e:
            raise ValueError('Issue while making query\n %s\n %s' % (str(repr(e)), query))
        _disconnect()
        return result


def getUserLogin():
    return getpass.getuser()
