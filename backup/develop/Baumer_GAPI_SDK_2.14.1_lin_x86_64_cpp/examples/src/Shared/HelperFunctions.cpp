/* Copyright 2019-2020 Baumer Optronic */
#include <iostream>
#include <string>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include "HelperFunctions.h"

#if defined(WITH_REGEX)
#   include <regex>  // C++11
#endif

#if defined(_WIN32)
    static const char c_cPathSeparator = '\\';
#else
    static const char c_cPathSeparator = '/';
#endif

void writeHeadLine(const std::string& sHeadLine, const char c_cChar) {
    std::cout << sHeadLine.c_str() << std::endl;
    std::cout << std::string(sHeadLine.size(), c_cChar).c_str() << std::endl << std::endl;
}

void writeHeader1(const std::string& sHeadLine, const char c_cChar) {
    std::cout << std::string(sHeadLine.size() + 4, c_cChar).c_str() << std::endl;
    std::cout << c_cChar << ' ' << sHeadLine.c_str() << ' ' << c_cChar << std::endl;
    std::cout << std::string(sHeadLine.size() + 4, c_cChar).c_str() << std::endl << std::endl << std::endl;
}


#if __cplusplus >= 201703L  // C++17 and greater

#include <filesystem>  // C++17

struct path_leaf_string {
    std::string operator()(const std::filesystem::directory_entry& entry) const {
        return entry.path().leaf().string();
    }
};

void readDirectory(const std::string& name, const std::string& /*filter*/, stringvec* v) {
    std::filesystem::path p(name);
    std::filesystem::directory_iterator start(p);
    std::filesystem::directory_iterator end;
    std::transform(start, end, std::back_inserter(&v), path_leaf_string());
}
#elif defined(_WIN32)
// Windows subsystem
#include <windows.h>

void readDirectory(const std::string& name, const std::string& filter, stringvec* v) {
    std::string pattern(name);
#if defined(WITH_REGEX)
    pattern.append("\\*");
#else
    if (filter.empty())
            pattern.append("\\*");
    else
        pattern.append("\\" + filter);
#endif  // defined(WITH_REGEX)
    WIN32_FIND_DATA data;
    HANDLE hFind;
    if ((hFind = FindFirstFile(pattern.c_str(), &data)) != INVALID_HANDLE_VALUE) {
        do {
#if defined(WITH_REGEX)
            if (!filter.empty()) {
                std::string str(data.cFileName);
                std::regex r(filter);
                std::smatch m;

                std::regex_search(str, m, r);
#if __cplusplus >= 201103L || _MSC_VER >= 1800
                for (auto found : m)
                    if (found == data.cFileName)
                        v->push_back(data.cFileName);
#else
                for (auto it = m.begin(); it != m.end(); ++it)
                    if (it->str() == data.cFileName)
                        v->push_back(data.cFileName);
#endif

            } else {
                v->push_back(data.cFileName);
            }
#else
            v->push_back(data.cFileName);
#endif
        } while (FindNextFile(hFind, &data) != 0);
        FindClose(hFind);
    }
}
#else  // POSIX
// Gnu-Linux subsystem
#include <sys/types.h>
#include <dirent.h>
void readDirectory(const std::string& name, const std::string& filter, stringvec* v) {
    try {
        std::cout << "opendir cmd with " << name << std::endl;
        DIR* dirp = opendir(name.c_str());
        std::cout << "opendir ends with " << (dirp ? "dirp" : "NULL") << std::endl;
        if (dirp) {
            struct dirent * dp;
            while ((dp = readdir(dirp)) != NULL) {
#if defined(WITH_REGEX)  // C++11 and greater 201103L?
                if (!filter.empty()) {
                    std::string str(dp->d_name);
                    std::regex r(filter);
                    std::smatch m;

                    // std::regex_search(std::string(dp->d_name), m, std::regex(filter));
                    std::regex_search(str, m, r);
                    for (auto found : m)  // std::cout << v << std::endl;
                        if (found == dp->d_name)
                            v->push_back(dp->d_name);
                } else {
                    v->push_back(dp->d_name);
                }
#else
                filter = filter;  // unused variable
                v->push_back(dp->d_name);
#endif
            }
            closedir(dirp);
        } else {
            int errsv = errno;
            char* errstr = std::strerror(errsv);
            std::cout << "error in opendir with error " << (errstr ? errstr : "") << " (" << errsv << ")!" << std::endl;
        }
    }
    catch (...) {
        std::cout << "exception in readDirectory!" << std::endl;
    }
}
#endif

#if USE_OPENCV
void createColorImage(const std::string& sPathName, const cv::Size& frameSize, const cv::Scalar& color,
                      const int& iCount) {
    cv::Mat frame(frameSize, CV_8UC3, color);
    std::stringstream sFileName;
    sFileName << sPathName << "/Example" << std::setw(3) << std::setfill('0') << iCount << ".jpg";
    cv::imwrite(sFileName.str(), frame);
}

int createExampleImages(const std::string& sPathName, const cv::Size& frameSize) {
    int iReturn = 0;
    int iCount = 0;

    try {
        int step = 0x10;
        for (int i = 0; i <= 0x100; i += step)
            createColorImage(sPathName, frameSize, cv::Scalar(0, 0, i), ++iCount);
        for (int i = 0x100; i >= 0; i -= step)
            createColorImage(sPathName, frameSize, cv::Scalar(0, 0, i), ++iCount);

        for (int i = 0; i <= 0x100; i += step)
            createColorImage(sPathName, frameSize, cv::Scalar(0, i, 0), ++iCount);
        for (int i = 0x100; i >= 0; i -= step)
            createColorImage(sPathName, frameSize, cv::Scalar(0, i, 0), ++iCount);

        for (int i = 0; i <= 0x100; i += step)
            createColorImage(sPathName, frameSize, cv::Scalar(i, 0, 0), ++iCount);
        for (int i = 0x100; i >= 0; i -= step)
            createColorImage(sPathName, frameSize, cv::Scalar(i, 0, 0), ++iCount);

        for (int i = 0x100; i >= 0; i -= step) {
            createColorImage(sPathName, frameSize, cv::Scalar(0x100, 0x100, 0x100), ++iCount);
        }
    }
    catch(...) {
        iReturn = 1;
    }

    return iReturn;
}
#endif  // USE_OPENCV
