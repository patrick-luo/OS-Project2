Hello, znati!
server class 
	public server(){
	//不需要用随即数来产生端口号
	//是否需要使用Executor Server 来管理线程
	//server class register() 没必要返回nameserver的地址
	//java threads instance variable 在内存中的存放形式
	//server class 可以也implements sender和 receiver， 因为 server本身就是一个小秘线程，小秘和壮汉线程所做的事情不一样，所以他们的借口的实现也不一样，可以独立出来。 目前的实现是使用一个小秘线程，但是感觉很冗余
	//msg que好像咩有用处
	//需要使用thread pool吗