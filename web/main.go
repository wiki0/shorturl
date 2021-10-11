package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic/v7"
	"net/http"
)

var client *elastic.Client
var host = "http://114.67.105.20:9201/"

// 定义接收数据的结构体
type Login struct {
	// binding:"required"修饰的字段，若接收为空值，则报错，是必须字段
	User    string `form:"username" json:"user" uri:"user" xml:"user" binding:"required"`
	Pssword string `form:"password" json:"password" uri:"password" xml:"password" binding:"required"`
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

func main() {
	var err error
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL("http://114.67.105.20:9201/"))
	if err != nil {
		panic(err)
	}
	r := gin.Default()
	r.LoadHTMLGlob("web/view/*")
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{"title": "我是测试", "ce": "123456"})
	})
	r.POST("/loginForm", func(c *gin.Context) {
		// 声明接收的变量
		var form Login
		// Bind()默认解析并绑定form格式
		// 根据请求头中content-type自动推断
		if err := c.Bind(&form); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// 判断用户名密码是否正确
		if form.Pssword != "admin" {
			c.JSON(http.StatusBadRequest, gin.H{"status": "304"})
			return
		}
		//字段相等
		q := elastic.NewQueryStringQuery("name:" + form.User)
		var res *elastic.SearchResult
		res, err = client.Search("cz-data").Query(q).Do(context.Background())
		if err != nil {
			println(err.Error())
		}
		c.JSON(http.StatusOK, res)
	})
	r.Run()
}
