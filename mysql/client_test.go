package mysql

import (
	"flag"
	"fmt"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	conn "github.com/zhaochuanyun/go-mysql/client"
	"github.com/zhaochuanyun/go-mysql/mysql"
	"github.com/zhaochuanyun/go-mysql/test_util/test_keys"
)






var testHost = flag.String("host", "127.0.0.1", "MySQL server host")

// We cover the whole range of MySQL server versions using docker-compose to bind them to different ports for testing.
// MySQL is constantly updating auth plugin to make it secure:
// starting from MySQL 8.0.4, a new auth plugin is introduced, causing plain password auth to fail with error:
// ERROR 1251 (08004): Client does not support authentication protocol requested by server; consider upgrading MySQL client
// Hint: use docker-compose to start corresponding MySQL docker containers and add the their ports here
var testPort = flag.String("port", "3306", "MySQL server port") // choose one or more form 5561,5641,3306,5722,8003,8012,8013, e.g. '3306,5722,8003'
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

func Test(t *testing.T) {
	segs := strings.Split(*testPort, ",")
	for _, seg := range segs {
		Suite(&clientTestSuite{port: seg})
	}
	TestingT(t)
}

type clientTestSuite struct {
	c    *conn.Conn
	port string
}

func (s *clientTestSuite) SetUpSuite(c *C) {
	var err error
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	s.c, err = conn.Connect(addr, *testUser, *testPassword, "")
	if err != nil {
		c.Fatal(err)
	}

	_, err = s.c.Execute("CREATE DATABASE IF NOT EXISTS " + *testDB)
	c.Assert(err, IsNil)

	_, err = s.c.Execute("USE " + *testDB)
	c.Assert(err, IsNil)

	s.testConn_CreateTable(c)
	s.testStmt_CreateTable(c)
}

func (s *clientTestSuite) TearDownSuite(c *C) {
	if s.c == nil {
		return
	}

	s.testConn_DropTable(c)
	s.testStmt_DropTable(c)

	if s.c != nil {
		s.c.Close()
	}
}

func (s *clientTestSuite) testConn_DropTable(c *C) {
	_, err := s.c.Execute("drop table if exists mixer_test_conn")
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) testConn_CreateTable(c *C) {
	str := `CREATE TABLE IF NOT EXISTS mixer_test_conn (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	_, err := s.c.Execute(str)
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestConn_Ping(c *C) {
	err := s.c.Ping()
	c.Assert(err, IsNil)
}

// NOTE for MySQL 5.5 and 5.6, server side has to config SSL to pass the TLS test, otherwise, it will throw error that
//      MySQL server does not support TLS required by the client. However, for MySQL 5.7 and above, auto generated certificates
//      are used by default so that manual config is no longer necessary.
func (s *clientTestSuite) TestConn_TLS_Verify(c *C) {
	// Verify that the provided tls.Config is used when attempting to connect to mysql.
	// An empty tls.Config will result in a connection error.
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	_, err := conn.Connect(addr, *testUser, *testPassword, *testDB, func(c *conn.Conn) {
		c.UseSSL(false)
	})
	if err == nil {
		c.Fatal("expected error")
	}

	expected := "either ServerName or InsecureSkipVerify must be specified in the tls.Config"
	if !strings.Contains(err.Error(), expected) {
		c.Fatalf("expected '%s' to contain '%s'", err.Error(), expected)
	}
}

func (s *clientTestSuite) TestConn_TLS_Skip_Verify(c *C) {
	// An empty tls.Config will result in a connection error but we can configure to skip it.
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	_, err := conn.Connect(addr, *testUser, *testPassword, *testDB, func(c *conn.Conn) {
		c.UseSSL(true)
	})
	c.Assert(err, Equals, nil)
}

func (s *clientTestSuite) TestConn_TLS_Certificate(c *C) {
	// This test uses the TLS suite in 'go-mysql/docker/resources'. The certificates are not valid for any names.
	// And if server uses auto-generated certificates, it will be an error like:
	// "x509: certificate is valid for MySQL_Server_8.0.12_Auto_Generated_Server_Certificate, not not-a-valid-name"
	tlsConfig := conn.NewClientTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, false, "not-a-valid-name")
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	_, err := conn.Connect(addr, *testUser, *testPassword, *testDB, func(c *conn.Conn) {
		c.SetTLSConfig(tlsConfig)
	})
	if err == nil {
		c.Fatal("expected error")
	}
	if !strings.Contains(errors.ErrorStack(err), "certificate is not valid for any names") &&
		!strings.Contains(errors.ErrorStack(err), "certificate is valid for") {
		c.Fatalf("expected errors for server name verification, but got unknown error: %s", errors.ErrorStack(err))
	}
}

func (s *clientTestSuite) TestConn_Insert(c *C) {
	str := `insert into mixer_test_conn (id, str, f, e) values(1, "a", 3.14, "test1")`

	pkg, err := s.c.Execute(str)
	c.Assert(err, IsNil)
	c.Assert(pkg.AffectedRows, Equals, uint64(1))
}

func (s *clientTestSuite) TestConn_Select(c *C) {
	str := `select str, f, e from mixer_test_conn where id = 1`

	result, err := s.c.Execute(str)
	c.Assert(err, IsNil)
	c.Assert(result.Fields, HasLen, 3)
	c.Assert(result.Values, HasLen, 1)

	ss, _ := result.GetString(0, 0)
	c.Assert(ss, Equals, "a")

	f, _ := result.GetFloat(0, 1)
	c.Assert(f, Equals, float64(3.14))

	e, _ := result.GetString(0, 2)
	c.Assert(e, Equals, "test1")

	ss, _ = result.GetStringByName(0, "str")
	c.Assert(ss, Equals, "a")

	f, _ = result.GetFloatByName(0, "f")
	c.Assert(f, Equals, float64(3.14))

	e, _ = result.GetStringByName(0, "e")
	c.Assert(e, Equals, "test1")
}

func (s *clientTestSuite) TestConn_Escape(c *C) {
	e := `""''\abc`
	str := fmt.Sprintf(`insert into mixer_test_conn (id, str) values(5, "%s")`,
		mysql.Escape(e))

	_, err := s.c.Execute(str)
	c.Assert(err, IsNil)

	str = `select str from mixer_test_conn where id = ?`

	r, err := s.c.Execute(str, 5)
	c.Assert(err, IsNil)

	ss, _ := r.GetString(0, 0)
	c.Assert(ss, Equals, e)
}

func (s *clientTestSuite) TestConn_SetCharset(c *C) {
	err := s.c.SetCharset("gb2312")
	c.Assert(err, IsNil)

	err = s.c.SetCharset("utf8")
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) testStmt_DropTable(c *C) {
	str := `drop table if exists mixer_test_stmt`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute()
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) testStmt_CreateTable(c *C) {
	str := `CREATE TABLE IF NOT EXISTS mixer_test_stmt (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute()
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestStmt_Delete(c *C) {
	str := `delete from mixer_test_stmt`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute()
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestStmt_Insert(c *C) {
	str := `insert into mixer_test_stmt (id, str, f, e, u, i) values (?, ?, ?, ?, ?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	r, err := stmt.Execute(1, "a", 3.14, "test1", 255, -127)
	c.Assert(err, IsNil)

	c.Assert(r.AffectedRows, Equals, uint64(1))
}

func (s *clientTestSuite) TestStmt_Select(c *C) {
	str := `select str, f, e from mixer_test_stmt where id = ?`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	result, err := stmt.Execute(1)
	c.Assert(err, IsNil)
	c.Assert(result.Values, HasLen, 1)
	c.Assert(result.Fields, HasLen, 3)

	ss, _ := result.GetString(0, 0)
	c.Assert(ss, Equals, "a")

	f, _ := result.GetFloat(0, 1)
	c.Assert(f, Equals, float64(3.14))

	e, _ := result.GetString(0, 2)
	c.Assert(e, Equals, "test1")

	ss, _ = result.GetStringByName(0, "str")
	c.Assert(ss, Equals, "a")

	f, _ = result.GetFloatByName(0, "f")
	c.Assert(f, Equals, float64(3.14))

	e, _ = result.GetStringByName(0, "e")
	c.Assert(e, Equals, "test1")

}

func (s *clientTestSuite) TestStmt_NULL(c *C) {
	str := `insert into mixer_test_stmt (id, str, f, e) values (?, ?, ?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	result, err := stmt.Execute(2, nil, 3.14, nil)
	c.Assert(err, IsNil)

	c.Assert(result.AffectedRows, Equals, uint64(1))

	stmt.Close()

	str = `select * from mixer_test_stmt where id = ?`
	stmt, err = s.c.Prepare(str)
	defer stmt.Close()

	c.Assert(err, IsNil)

	result, err = stmt.Execute(2)
	b, err := result.IsNullByName(0, "id")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, false)

	b, err = result.IsNullByName(0, "str")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, true)

	b, err = result.IsNullByName(0, "f")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, false)

	b, err = result.IsNullByName(0, "e")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, true)
}

func (s *clientTestSuite) TestStmt_Unsigned(c *C) {
	str := `insert into mixer_test_stmt (id, u) values (?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)
	defer stmt.Close()

	result, err := stmt.Execute(3, uint8(255))
	c.Assert(err, IsNil)
	c.Assert(result.AffectedRows, Equals, uint64(1))

	str = `select u from mixer_test_stmt where id = ?`

	stmt, err = s.c.Prepare(str)
	c.Assert(err, IsNil)
	defer stmt.Close()

	result, err = stmt.Execute(3)
	c.Assert(err, IsNil)

	u, err := result.GetUint(0, 0)
	c.Assert(err, IsNil)
	c.Assert(u, Equals, uint64(255))
}

func (s *clientTestSuite) TestStmt_Signed(c *C) {
	str := `insert into mixer_test_stmt (id, i) values (?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)
	defer stmt.Close()

	_, err = stmt.Execute(4, 127)
	c.Assert(err, IsNil)

	_, err = stmt.Execute(uint64(18446744073709551516), int8(-128))
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestStmt_Trans(c *C) {
	_, err := s.c.Execute(`insert into mixer_test_stmt (id, str) values (1002, "abc")`)
	c.Assert(err, IsNil)

	err = s.c.Begin()
	c.Assert(err, IsNil)

	str := `select str from mixer_test_stmt where id = ?`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute(1002)
	c.Assert(err, IsNil)

	err = s.c.Commit()
	c.Assert(err, IsNil)

	r, err := stmt.Execute(1002)
	c.Assert(err, IsNil)

	str, _ = r.GetString(0, 0)
	c.Assert(str, Equals, `abc`)
}