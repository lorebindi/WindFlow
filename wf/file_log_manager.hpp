//
// Created by bindi on 8/30/24.
//

#ifndef FILELOGMANAGER_H
#define FILELOGMANAGER_H

#include <string>
#include <mutex>
#include <fstream>
#include <iostream>

using namespace std;

/**
 * This class manage the writing of couple (replica-core). This class implements the singleton pattern.
 * */


namespace wf {
    class file_log_manager {

    public:

        /**
         * Return the reference of the single instance.
         * */
        static file_log_manager& getInstance(){
            static file_log_manager singleManager;
            return singleManager;
        }

        /**
         * Method used to write a log.
         * */
        ssize_t log(std::string& name, ssize_t replicaIndex, int cpuId) {
            logMutex.lock();
            // Scrittura
            if (logFile.is_open()) {
                logFile << "cpu(" << cpuId << "), " << name << "(" << replicaIndex << ")" <<  endl;
                logMutex.unlock();
                return 0;
            }
            logMutex.unlock();
            return -1;
        }

        /**
        * Singletons should not be cloneable.
        */
        file_log_manager(file_log_manager &other) = delete;
        /**
         * Singletons should not be assignable.
         */
        void operator=(const file_log_manager &) = delete;




    private:

        file_log_manager(){
            const std::string filePath = "log/mapping.log";
            logFile.open(filePath, std::ios::out); // Apri in modalità append

            if (logFile.is_open()) {
                std::cout << "File di log aperto: " << filePath << std::endl;
            } else {
                std::cerr << "Errore nell'apertura del file di log: " << filePath << std::endl;
            }
        }

        ~file_log_manager() {
            if (logFile.is_open()) {
                logFile.close();
            }
        }

        ofstream logFile;
        mutex logMutex;

    };

}

#endif //FILELOGMANAGER_H