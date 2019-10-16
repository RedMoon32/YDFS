//
// Created by rinat on 17.10.2019.
//

#ifndef RUNMASTER
#define RUNMASTER

#include <iostream>
#include <string>
#include "masterNode.h"

using namespace std;
using namespace httplib;

int LOCAL_DATA_NODE = 2020;

void runMaster() {

    Server master;

    master.Get("/ping", [](const Request &req, Response &res) {
        res.set_content("Hello, Master Node is Alive!", "text/plain");
    });

    //parameters passed via url - /fileChunks?file=kek
    master.Get("/fileChunks", [](const Request &req, Response &res) {
        string fileName = req.params.find("file")->second;
        res.set_content("[ \"1 - 10.0.0.11\", \"2 - 10.0.0.12\"]", "application/json");
    });

    master.set_logger([](const Request &req, const Response &res) {
        cout << req.method << " " << req.path << endl;
    });

    cout << "Master Node is running on 3030 port\n";

    master.listen("localhost", 3030);

//    master.Get(R"(/numbers/(\d+))", [&](const Request &req, Response &res) {
//        auto numbers = req.matches[1];
//        res.set_content(numbers, "text/plain");
//    });
//
//    master.Get("/stop", [&](const Request &req, Response &res) {
//        master.stop();
//    });

}

void pingDataNodes() {
    sleep(5);
    httplib::Client cli("localhost", LOCAL_DATA_NODE);
    int i = 0;
    while (i < 10) {
        cout << "Ping to datanode http://localhost:" << LOCAL_DATA_NODE << " has been sent" << endl;
        auto res = cli.Get("/ping");
        if (res && res->status == 200) {
            i++;
            cout << "Ping successfull, result:" << res->body << endl;
        } else {
            cout << "Ping unsuccessfull" << endl;
        }
        sleep(5);
    }
}

#endif