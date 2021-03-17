package main

import (
	"fmt"
	"net"
	"strings"
	"time"
)


//将所有代码写在一个文件中，不做代码整理

type User struct {
	name string
	id string
	msg chan string
}

//创建一个全局的map结构，用于保存所有的用户
var allUsers = make(map[string]User)

//定义一个message全局通道，用接收任何人发送过来的消息
var message = make(chan string,10)

func main()  {
	//创建服务器
	listener,err := net.Listen("tcp",":8080")
	if err != nil {
		fmt.Println("net.Listen err:",err)
		return
	}

	//启动全局唯一的go程，负责监听message通道，写给所有的用户
	go broadcast()

	fmt.Println("服务器启动成功!")

	for {
		fmt.Println("=======>主go程监听中...")

		//监听
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("listener.Accept err", err)
			return
		}
		//建立连接
		fmt.Println("建立连接成功！")
		//启动处理业务的go程
		go handler(conn)
	}
}

//处理具体业务
func handler(conn net.Conn)  {

		fmt.Println("启动业务...")

		//客户端与服务器建立连接的时候，会有ip和port，因此将port当成user id
		//创建user
		clientAddr := conn.RemoteAddr().String()
		//fmt.Println("chiientAddr: ",clientAddr[10:])
		newUser := User{
			name: "虎扑JR"+clientAddr[10:],//可以修改，会提供rename命令修改
			id: clientAddr, //id不会修改，作为map中的key
			msg: make(chan string,10),//注意要make空间，否则无法写数据
		}
		//添加user到map结构
		allUsers[newUser.id] = newUser

		//定义一个退出信号，用户监听client退出
		var isQuit = make(chan bool)
		//创建一个用于重置计数器的管道，用于告知watch函数，当前用户正在输入
		var restTimer = make(chan bool)
		//启动go程，负责监听退出信号
		go watch(&newUser,conn,isQuit,restTimer)

		//启动go程，负责将msg数据返回给客户端
		go writeBackToClient(&newUser,conn)

		//向message写入数据，当前用户上线的消息，用于通知所有人
		loginInfo := fmt.Sprintf("[%s]:[%s] ===> 上线了login！！\n",newUser.id,newUser.name)
		message <- loginInfo

	for {
		//具体业务逻辑
		buf :=make([]byte,1024)

		//读取客户端发送过来的数据
		cnt,err := conn.Read(buf)
		if cnt == 0 {
			fmt.Println("客户端主动关闭ctrl+c，准备退出！")
			//map删除用户，conn close
			//服务器还可以主动的退出
			//在这里不进行真正的退出动作，而是发送一个退出信号，统一做退出处理，可以使用新的管道做信号传递
			isQuit <- true
		}
		if err != nil {
			fmt.Println("conn.Read err:",err,",cnt:",cnt)
			return
		}
		fmt.Println("服务器接受客户端发送过来的数据为：",string(buf[:cnt-1]),",cnt: ",cnt)

		//业务逻辑处理 开始-----------
		//1.查询当前所有的用户 who
		//判断接受的数据是不是who ==> 长度&字符串
		userInput := string(buf[:cnt-1]) //这是用户输入的数据，最后一个是回车，去掉回车
		if len(userInput)==4 && userInput == "\\who" {
			//遍历allUsers这个map：（key: userid value：user本身）
			fmt.Println("用户即将查询所有用户信息！")

			//这个切片包含所有的用户信息
			var userInfos []string

			for _,user := range allUsers {
				userInfo := fmt.Sprintf("userid:%s,username:%s",user.id,user.name)
				userInfos = append(userInfos,userInfo )
			}
			//最终写到管道中，一定是一个字符串
			r := strings.Join(userInfos,"\n") //连接数字切片，生成字符串
			//将数据返回给查询的客户端
			newUser.msg <- r
		} else if len(userInput) >9 && userInput[:7]=="\\rename" {
			//规则：rename|ddd
			//读取数据判断长度，判断字符是rename
			//使用|分割，获取|后面的内容，作为名字
			//更新用户名字 newUser.name = ddd
			newUser.name = strings.Split(userInput,"|")[1]
			allUsers[newUser.id] = newUser //更新map中的user
			//通知客户端，更新成功
			newUser.msg <- "rename successfully!"
		} else {
			//如果用户输入的不是命令，只是普通的聊天信息，那么只需要写入到广播通道中，由其他的go程进行常规转发
			message <- userInput
		}
		restTimer <- true

		//业务逻辑处理 结束-----------

	}
}


//向所有的用户广播消息，启动一个全局唯一的go程
func broadcast()  {
	fmt.Println("广播go程启动成功...")

	for {

		//1.从message中读取数据
		info := <-message
		fmt.Println("message接收到的消息：", info)
		//2.将数据写入到每一个用户的msg管道中
		for _, user := range allUsers {
			//如果msg是非缓冲的，那么会在这里阻塞
			user.msg <- info
		}
	}
}

//每个用户应该还有一个用来监听自己msg管道的go程，负责将数据返回给客户端。
func writeBackToClient(user *User,conn net.Conn)  {
	fmt.Printf("user ： %s的go程正在监听自己的msg管道\n",user.name)
	for data := range user.msg {
		fmt.Printf("user:%s写回给客户端的数据为:%s\n",user.name,data)
		_,_ = conn.Write([]byte(data+"\n"))
	}
	
}

//启动一个go程，负责监听退出信号，触发后进行清理工作：delete map，close conn都在这里处理
func watch(user *User,conn net.Conn,isQuit <- chan bool,restTimer <-chan bool)  {
	fmt.Println("启动监听退出信号的go程...")
	defer fmt.Println("watch go程退出！")
	for {
		select {
		case <- isQuit:
			logoutInfo := fmt.Sprintf("%s exit already!\n",user.name)
			fmt.Println("删除当前用户：",user.name)
			delete(allUsers,user.id)
			message <- logoutInfo
			conn.Close()
			return
		case <- time.After(10*time.Second):
			logoutInfo := fmt.Sprintf("%s timeout exit already!\n",user.name)
			fmt.Println("删除当前用户：",user.name)
			delete(allUsers,user.id)
			message <- logoutInfo
			conn.Close()
			return
		case <- restTimer:
			fmt.Printf("连接%s 重置计数器！\n",user.name)
			
		}
	}
}