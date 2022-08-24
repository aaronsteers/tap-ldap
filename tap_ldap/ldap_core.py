# https://stackoverflow.com/questions/4784775/ldap-query-in-python

import ldap
import singer


LDAP_QUERIES = {"user": "(&(objectCategory=person)(objectClass=user))"}
SCHEMA_NAMES = ["user", "domain"]


def connect():
    ldap_conn = ldap.initialize("ldap://ldap.myserver.com:389")
    bind_dn = "cn=myUserName,ou=GenericID,dc=my,dc=company,dc=com"
    pw = "myPassword"
    base_dn = "ou=UserUnits,dc=my,dc=company,dc=com"
    bas_dn = "CN=Users,DC=slalom,DC=com"

    try:
        ldap_conn.protocol_version = ldap.VERSION3
        ldap_conn.simple_bind_s(bind_dn, pw)
    except ldap.INVALID_CREDENTIALS:
        print("Your username or password is incorrect.")
        sys.exit(0)
    except ldap.LDAPError as e:
        if type(e.message) == dict and e.message.has_key("desc"):
            print(e.message["desc"])
        else:
            print(e)
        sys.exit(0)
    return ldap_conn


def disconnect(ldap_conn):
    ldap_conn.unbind_s()


def detect_schema(schema_name):
    ldap_conn = connect()
    query_result = run_query(ldap_conn=ldap_conn, search_query="", attribute_list=["*"])
    disconnect(ldap_conn)
    return


def get_all_data(stream_name):
    ldap_conn = connect()
    return run_query(
        ldap_conn, search_query=LDAP_QUERIES[stream_name], attribute_list=["*"]
    )


def run_query(
    ldap_conn,
    search_query="(&(gidNumber=123456)(objectClass=posixAccount))",
    attribute_list=["mail", "department"],
):
    # this will scope the entire subtree under UserUnits
    search_scope = ldap.SCOPE_SUBTREE
    # Bind to the server
    try:
        ldap_result_id = ldap_conn.search(
            base_dn, search_scope, search_query, attribute_list
        )
        result_set = []
        while 1:
            result_type, result_data = ldap_conn.result(ldap_result_id, 0)
            if result_data == []:
                break
            ## if you are expecting multiple results you can append them
            ## otherwise you can just wait until the initial result and break out
            if result_type == ldap.RES_SEARCH_ENTRY:
                yield result_data
    except ldap.LDAPError as e:
        print(e)
    disconnect(ldap_conn)
