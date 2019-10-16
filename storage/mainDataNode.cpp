#include <iostream>
#include "dataNode.h"
#include <thread>

int main() {
    std::thread dServer(runDataNode);
    dServer.join();
    return 0;
}