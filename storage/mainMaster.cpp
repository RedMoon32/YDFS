#include <iostream>
#include "masterNode.h"


int main() {
    std::thread mServer(runMaster);
    pingDataNodes();
    mServer.join();
    return 0;
}