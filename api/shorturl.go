package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/olivere/elastic/v7"
	"strconv"
	"time"
)

type PagePerson struct {
	Id        *string `db:"id"`
	Username  *string `db:"xm"`
	Sex       *string `db:"xb"`
	Birthday  *string `db:"csrq"`
	Telephone *string `db:"gddh"`
	Phone     *string `db:"yddh"`
	ID        *string `db:"zjhm"`
	DIBAO     int     `db:"isDb"`
	CANJI     int     `db:"isCj"`
	DAIYE     int     `db:"isDy"`
	SHIYE     int     `db:"isSy"`
	DJLR      int     `db:"isdjlr"`
	TUIXIU    int     `db:"isTx"`
}

type Person struct {
	Name      string `json:"name"`
	Sex       string `json:"sex"`
	Birthday  string `db:"birthday"`
	Telephone string `db:"telephone"`
	Phone     string `db:"phone"`
	ID        string `db:"ID"`
	Label     string `json:"label"`
}

var Db *sqlx.DB
var Es *elastic.Client

func init() {

	database, err := sqlx.Open("mysql", "yf:wiki5620@tcp(114.67.105.20:3306)/test")
	if err != nil {
		// Handle error
		panic(err)
	}

	Db = database
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL("http://114.67.105.20:9200/"))
	if err != nil {
		panic(err)
	}
	Es = client
}

func transBlank(str *string) string {
	if str != nil && *str != "" {
		return *str
	} else {
		return ""
	}
}
func transBlankDay(per PagePerson) string {
	if per.Birthday != nil && *per.Birthday != "" {
		return *per.Birthday
	} else {
		s := *per.ID
		return s[6:10] + "-" + s[10:12] + "-" + s[12:14]
	}
}

func transSex(str *string) string {
	if "XB1" == *str {
		return "男"
	} else {
		return "女"
	}
}

func transLabel(per PagePerson) string {
	var labs []string
	if 1 == per.DIBAO {
		labs = append(labs, "低保")
	} else if 1 == per.CANJI {
		labs = append(labs, "残疾")
	} else if 1 == per.DAIYE {
		labs = append(labs, "待业")
	} else if 1 == per.DJLR {
		labs = append(labs, "独居")
	} else if 1 == per.TUIXIU {
		labs = append(labs, "退休")
	} else if 1 == per.SHIYE {
		labs = append(labs, "失业")
	}
	if len(labs) == 0 {
		return ""
	} else {
		s, _ := json.Marshal(labs)
		return string(s)
	}
}

func bulkPushEsEmpty(client *elastic.Client, ch chan Person) {
	ctx := context.Background()
	bulkRequest := client.Bulk()
	if 0 < len(ch) && len(ch) < 800 {
		go func() {
			for i := 1; i <= len(ch); i++ {
				esRequest := elastic.NewBulkIndexRequest().Index("person-data").Doc(<-ch)
				bulkRequest = bulkRequest.Add(esRequest)
			}
			_, err := bulkRequest.Do(ctx)
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	time.Sleep(10 * time.Second)
}

func bulkPushEs(client *elastic.Client, ch chan Person) {
	ctx := context.Background()
	bulkRequest := client.Bulk()
	if len(ch) >= 800 {
		go func() {
			for i := 1; i <= len(ch); i++ {
				esRequest := elastic.NewBulkIndexRequest().Index("person-data").Doc(<-ch)
				bulkRequest = bulkRequest.Add(esRequest)
			}
			_, err := bulkRequest.Do(ctx)
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	//fmt.Printf("bulkPushEs %d\n", len(ch))
	time.Sleep(800 * time.Millisecond)
}

func main() {
	i := 0
	ch := make(chan Person, 30000)
	go func() {
		for {
			bulkPushEs(Es, ch)
		}
	}()
	go func() {
		for {
			bulkPushEsEmpty(Es, ch)
		}
	}()
	for true {
		fmt.Println(i)
		var pagePerson []PagePerson
		err := Db.Select(&pagePerson, "select * from page_person limit ?,?", i*5000, 5000)
		if err != nil {
			fmt.Printf("query faied, error:[%v]", err.Error())
			return
		}
		if len(pagePerson) > 0 {
			fmt.Println(strconv.Itoa(len(pagePerson)))
			for _, value := range pagePerson {
				per := Person{
					Name:      transBlank(value.Username),
					Sex:       transSex(value.Sex),
					Birthday:  transBlankDay(value),
					Telephone: transBlank(value.Telephone),
					Phone:     transBlank(value.Phone),
					ID:        transBlank(value.ID),
					Label:     transLabel(value),
				}
				ch <- per
			}
		} else {
			fmt.Println("==============end============")
		}
		i++
		time.Sleep(1 * time.Second)
	}

}
