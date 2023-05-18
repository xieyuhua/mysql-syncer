package mysql

import (
	"bytes"
	"fmt"
	"sync"
// 	"time"
	"github.com/siddontang/go-log/log"
    "github.com/pingcap/errors"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

type Client struct {
	conn *sql.DB
	thread   int
}

// ClientConfig is the configuration for the client.
type ClientConfig struct {
	Addr     string
	User     string
	Password string
	Schema   string
	Table    string
	Thread   int
	MaxConnect  int
	MaxOpen     int
}

// NewClient creates the Cient with configuration.
func NewClient(conf *ClientConfig) *Client {
	c := new(Client)
	connString := conf.User + ":" + conf.Password + "@tcp(" + conf.Addr + ")/" + conf.Schema
	c.conn, _ = sql.Open("mysql", connString)
	//最大打开的连接数
	c.conn.SetMaxOpenConns(conf.MaxOpen)
	//最大连接数
	c.conn.SetMaxIdleConns(conf.MaxConnect)
	c.thread = conf.Thread
	return c
}

const (
	ActionInsert = "insert"
	ActionUpdate = "update"
	ActionDelete = "delete"
)

// BulkRequest is used to send multi request in batch.
type BulkRequest struct {
	Action string
	Schema string
	Table  string
	Data   map[string]interface{}

	PkName  string
	PkValue interface{}
}


//保证前段走完
var m = make(chan bool, 1)

// Bulk sends the bulk request.
func (c *Client) Bulk(reqs []*BulkRequest)  (interface{} ,error) {
    
    // 阻塞等待     全量和增量  判断
    m <- true
    
    
    //线程池
    var chans = make(chan bool, c.thread)
    
    //任务执行等待完
    var wg sync.WaitGroup 
    
    //是否执行错误
    var ddi error
    ddi = nil
    
	//限制多线程执行
	for i, req := range reqs {
	    chans <- true
	    //占位
        go func(i int,req *BulkRequest,c *Client, wg *sync.WaitGroup) {
            wg.Add(1)
            // log.Infof("************ success --> %v", i)
            // time.Sleep(4 * time.Second) 
            bufs := make([]bytes.Buffer, len(reqs))
    		if err := req.bulk(&bufs[i], c); err != nil {
    		    <-chans
    		    wg.Done()
    		    ddi = errors.Trace(err)
    		}
    		wg.Done()
    		<-chans
        }(i,req,c,&wg)
	}
    // 等待所有的任务完成
    wg.Wait()
    if ddi != nil {
        return nil, ddi
    }
    
    //放新的一组进入
    <-m
    
	return nil, nil
}
//执行更新
func (r *BulkRequest) bulk(buf *bytes.Buffer, c *Client) error {
	
	
	switch r.Action {
	case ActionDelete:
		// for delete
		buf.WriteString(" DELETE FROM ")
		buf.WriteString(r.Schema + "." + r.Table)
		buf.WriteString(" WHERE " + r.PkName + " = " + trans(r.PkValue))
		
		log.Infof("Execute success --> %v", buf.String())
    	
    	_, err := c.conn.Exec(buf.String())
    	if err != nil {
            // log.Errorf("Execute Error! --> %v", err)
            return errors.Trace(err)
    	}
    	
	case ActionUpdate:
// 	INSERT INTO test_unique_key ( `id`, `name`, `term_id`, `class_id`, `course_id` ) VALUES( '17012', 'cate', '172012', '170', '1711' ) ON DUPLICATE KEY UPDATE name = '张三1',course_id=32
		keys := make([]string, 0, len(r.Data))
		values := make([]interface{}, 0, len(r.Data))
		for k, v := range r.Data {
			keys   = append(keys, k)
			values = append(values, v)
		}
    	if len(keys) == 0 {
    		break;
    	}
    	
		buf.WriteString(" UPDATE `")
		buf.WriteString(r.Schema + "`.`" + r.Table + "` SET `")
		buf.WriteString(keys[0] + "` = ? ")
		
		for _, v := range keys[1:] {
			buf.WriteString(", `" + v + "` = ?")
		}
		buf.WriteString(" WHERE `" + r.PkName + "` = " + trans(r.PkValue))
		
		log.Infof("Execute success --> %v", buf.String())
// 		log.Infof("Execute success --> %v", values)
		
    	stmtIns, err := c.conn.Prepare(buf.String())
    	if err != nil {
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	defer stmtIns.Close()
    	_, err = stmtIns.Exec(values...)
    	if err != nil {
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
		
	default:
		// for insert
		value := make([]interface{}, 0, len(r.Data)*2)
		
		fields  := ""
		values  := ""
		fields2 := ""
		
		for k, v := range r.Data {
		    value = append(value, v) 
			if fields == ""{
				fields = "`"+k+"`"
				values = "?"
				fields2 = k+"= ?"
			}else{
				fields += ",`"+k+"`"
				values += ",?"
				fields2 += ","+k+"= ?"
			}
		}

    	if len(value) == 0 {
    		break;
    	}
		//重复事情
		for _, v := range value {
		    value = append(value, v) 
		}
		
// 		sql := "REPLACE INTO "+r.Schema + "."+r.Table+" ("+fields+") VALUES ("+values+")"
		sql := "INSERT INTO "+r.Schema + "."+r.Table+" ("+fields+") VALUES ("+values+") ON DUPLICATE KEY UPDATE " + fields2
        //执行
		log.Infof("Execute success --> %v", sql)
// 		log.Infof("Execute success --> %v", value)
		
    	stmtIns, err := c.conn.Prepare(sql)
    	if err != nil {
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	defer stmtIns.Close()
    	_, err = stmtIns.Exec(value...)
    	if err != nil {
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
	}
	return nil
}


func trans(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case string:
        return fmt.Sprintf("\"%v\"", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
