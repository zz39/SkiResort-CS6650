package com.skiresort.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class YourController {

	@GetMapping("/")  // or whatever path you want to handle
	public String handleRequest() {
		return "Hello World!";  // or your desired response
	}

	@GetMapping("/skier")  // or whatever path you want to handle
	public String handleRequest2() {
		return "You are a skier!";  // or your desired response
	}

	@GetMapping("/resort")  // or whatever path you want to handle
	public String handleRequest3() {
		return "You are at a resort!";  // or your desired response
	}
}
