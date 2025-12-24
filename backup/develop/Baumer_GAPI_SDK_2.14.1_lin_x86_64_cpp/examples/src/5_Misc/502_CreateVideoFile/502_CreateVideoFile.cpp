/* Copyright 2019-2020 Baumer Optronic */
#include <stdio.h>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <set>
#include <vector>
#include <algorithm>
#include <string>
#include <functional>

// This example shows how to convert a set of images (in alphabetical order) into a video file using the openCV
// library. If no command line parameters are given the example will generate a set of images in the folder
// 'ExampleImages' (if not empty the images will be used) and create a video file 'ExampleVideo.avi' from this
// images

// openCV must be available!!!

// Command Line Parameters
//
// --imgDir: sets the directory where source images are located, if not set, the default 'ExampleImages' will be used
// Important: All image files need to have the same dimension for the video creation to be successful
// Tip: You can use the  Baumer CameraExplorer Image Recorder functionality to record some example images
//
// --videoFile: sets the output file name for the created video, if not, the default 'ExampleVideo.avi' will be used

const std::string gc_sImageDirDefault = "ExampleImages";
const std::string gc_sVideoFileNameDefault = "ExampleVideo.avi";

#if defined(_WIN32)
#   include <direct.h>
#else
#   include <sys/stat.h>
#endif
#include "Arguments.h"
#include "HelperFunctions.h"

#define EXAMPLE_IMAGES 1

#if defined(_WIN32)
static const char c_cPathSeparator = '\\';
#else
static const char c_cPathSeparator = '/';
#endif

// ---------------------------------------------------------------------------------------------------------------------
// Here you can use argument parameters for sImgDirectory and sVideoFileName!
int main(int argc, char* argv[]) {
    std::string sImgDirectory;
    std::string sVideoFileName;
    static const Argument argumentList[] = {
        { &sImgDirectory,    "p", "imgPath",   false   , argumentString,     0, "Pathname",     "Image Path" },
        { &sVideoFileName,   "v", "videoFile", false   , argumentString,     0, "Filename", "Video File Name" },
    };
    parseArguments(argumentList, (sizeof(argumentList)/sizeof(argumentList[0])), argc, argv);

    int iReturnCode = 0;
    try {
        std::cout << std::endl;
        std::string sExampleName = __FILE__;
        sExampleName = sExampleName.substr(sExampleName.find_last_of(c_cPathSeparator) + 1);

        writeHeader1(sExampleName);
        writeHeader1("EXAMPLE VIDEO FROM IMAGE DIRECTORY");

// The flag USE_OPENCV is set by the CMake run, CMake tries to find OpenCV >= Version 2
#if USE_OPENCV  // OpenCV

// The flag USE_OCL_COMPONENT is set by the CMake run if OpenCV can use OpenCL to speed up it's functionality
#   if USE_OCL_COMPONENT
        if (cv::ocl::haveOpenCL()) {
            cv::ocl::Context context;
            if (!context.create(cv::ocl::Device::TYPE_GPU)) {
                // FAIL: no context
            } else {
                std::cout << "OpenCL available!" << std::endl;

                for (size_t i = 0; i < context.ndevices(); i++) {
                    cv::ocl::Device clDevice = context.device(i);
                    std::cout << clDevice.vendorName() << std::endl;
                    std::cout << clDevice.driverVersion() << std::endl << std::endl;
                }
            }
        }
#   endif  // USE_OCL_COMPONENT
        stringvec fileList;

        std::string imgDir;
        if (!sImgDirectory.empty()) {
            imgDir = sImgDirectory;
        } else {
            imgDir = gc_sImageDirDefault;
            std::cout << "Using default setting for 'imgPath': " << imgDir << std::endl;
            // Create the directory below the execution dir
#if defined(_WIN32)
            _mkdir(imgDir.c_str());
#else
            mkdir(imgDir.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
#endif
        }
#ifdef WITH_REGEX
        readDirectory(imgDir, ".*.jpg", &fileList);
#else
        readDirectory(imgDir, "*.jpg", &fileList);
#endif
        size_t slashPos = imgDir.find_last_of("/\\");
        std::string exampleDir = imgDir;
        if (slashPos < std::string::npos)
            exampleDir = imgDir.substr(slashPos+1);

        if (fileList.empty() && (exampleDir == gc_sImageDirDefault)) {
            // Create some images if the image directory identical with the default
            std::cout << "Here were be some test images with changing colors created!" << std::endl;
            createExampleImages(imgDir, cv::Size(1000, 500));
            readDirectory(imgDir, ".*.jpg", &fileList);
        }

        // If the fileList is empty, exit the program:
        if (fileList.empty()) {
            std::cout << "No Image(s) found!" << std::endl;
            return 102;
        }
        std::sort(fileList.begin(), fileList.end(), std::less<std::string>());

        // Now there are some pictures in the list - and you can create a video file from this
        cv::Mat frame = cv::imread(imgDir + "/" + fileList.at(0));
        cv::Size frameSize = cv::Size(frame.cols, frame.rows);

        int frames_per_second = 10;
        // Create and initialize the VideoWriter object:
        // std::string sVideoFileName;
        if (sVideoFileName.empty()) {
            sVideoFileName = gc_sVideoFileNameDefault;
            std::cout << "Using default setting for 'videoFile': " << sVideoFileName << std::endl;
        }
#ifdef CV_FOURCC_MACRO
        cv::VideoWriter oVideoWriter(sVideoFileName,
            CV_FOURCC('M', 'J', 'P', 'G'),  // cv::VideoWriter::fourcc
            frames_per_second, frameSize, true);
#else
        cv::VideoWriter oVideoWriter(sVideoFileName,
            cv::VideoWriter::fourcc('M', 'J', 'P', 'G'),
            frames_per_second, frameSize, true);
#endif

        // If the VideoWriter object is not initialized successfully, exit the program:
        if (oVideoWriter.isOpened() == false) {
            std::cout << "Cannot save the video to a file" << std::endl;
            return 103;
        }

        // ... and now write all pictures in the video file
        int iNumber = 0;
        for (std::string filename : fileList) {
            std::cout << "file" << (++iNumber) << ": " << filename << std::endl;
            cv::Mat frame2 = cv::imread(imgDir + "/" + filename);
            if (!frame2.empty())  // use valid picture only!
                oVideoWriter.write(frame2);
        }
        oVideoWriter.release();

#else  // USE_OPENCV
        // If we could not find OpenCV this text will be displayed.
        std::cout << "Without OpenCV we cannot create the video in this example!" << std::endl;
        std::cout << "Availability is checked while CMake creates this project." << std::endl;
        std::cout << "Please install OpenCV (version 2.3 or later) or set 'OpenCV_DIR' to the" << std::endl;
        std::cout << "correct path in the CMakeTests.txt script or as a variable in your environment" << std::endl;
        std::cout << "and run CMake again. " << std::endl;
        std::cout << "######################################" << std::endl << std::endl;

#endif  // USE_OPENCV

        std::cout << std::endl;
        std::cout << "End" << std::endl << std::endl;
    }
    catch (const std::exception& e) {
        iReturnCode = (iReturnCode == 0) ? 1 : iReturnCode;
        std::cout << "unhandled std exception: '" << e.what() << "'" << std::endl;
    }
    catch (...) {
        iReturnCode = (iReturnCode == 0) ? 1 : iReturnCode;
        std::cout << "unhandled exception" << std::endl;
    }

    if (iReturnCode != 0)
        std::cout << "Example ends with error: " << iReturnCode << std::endl;

    std::cout << "Press ENTER to close the program!" << std::endl;
    int endKey = std::cin.get();
    endKey = endKey;  // unused variable
    return iReturnCode;
}
