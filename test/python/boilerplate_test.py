import duckdb

def test_boilerplate():
    conn = duckdb.connect('');
    conn.execute('SELECT boilerplate() as value;');
    res = conn.fetchall()
    assert(res[0][0] == "I'm a boilerplate!");