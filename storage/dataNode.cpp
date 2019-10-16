//
// Created by rinat on 17.10.2019.
//

#ifndef RUNMASTER
#define RUNMASTER

#include <iostream>
#include <string>
#include "dataNode.h"

using namespace std;
using namespace httplib;

void runDataNode() {

    Server slave;

    slave.Get("/ping", [](const Request &req, Response &res) {
        res.set_content("Hello, Data Node is Alive!", "text/plain");
    });

    slave.set_logger([](const Request &req, const Response &res) {
        cout << req.method << " " << req.path << endl;
    });
    
    cout << "Data Node is running on 2020 port\n";
    
    slave.listen("localhost", 2020);

}

#endif