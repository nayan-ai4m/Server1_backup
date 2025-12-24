/* Copyright 2019-2020 Baumer Optronic */

#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <thread>
#include <sstream>
#include <atomic>
#include <map>
#include <ctime>
#include <limits>
#include <cctype>
#include <chrono>
#include <string>
#include <vector>
#include <list>

#include "CameraManager.h"
#include "DoubleBufferHandler.h"
#include "BufferInformation.h"

// -------------
// the following declarations are used to control the status command and the calculation command
// -------------

// a flag which controls the status command
std::atomic<bool> command_status(false);
// a flag which controls the calculation command
std::atomic<bool> command_calculation(false);
// a flag to enable the output of camera streaming information
bool camera_streaming_information(false);
// a flag to enable the output of buffer information
bool buffer_information(false);
// a flag to enable the output of logging information
bool logging_information(false);
// a flag to activate the heart beat command
bool set_heartbeat(false);
// a flag to disable the heart beat for GEV cameras
bool disable_heartbeat(false);

// this function activates the feature command on all connected cameras
// the feature command use the ExposureTime feature of the camera
// the parameter passes the new exposure time to be written to the cameras
void StartFeatureCommandExposure(CameraManager * camera_manager, double exposure_time);

// this function activates the capture command on all connected cameras
// the parameter passes the number of images to be captured
void StartCaptureCommand(CameraManager * camera_manager, unsigned int images);

// -------------
// thread control, finish flag and thread functions
// -------------

// a flag which is used to stop all the used threads at once
bool finish_console_threads = false;

// the thread routine of the status command is used to print status information about the current buffer,
// exposure time, logging information and calculation results
// prints the status information to the console
void StatusThreadRoutine(CameraManager* camera_manager);

// the thread routine of the calculation command is used to make a image calculation
// prints the calculation result to the console
void CalculationThreadRoutine(CameraManager* camera_manager);

// -------------
// declaration of synchronisation functions needed for synchronized buffer access between the application threads
// -------------

// this function is used to check if new data is available
bool HasDoubleBufferHandlerNewData(DoubleBufferHandler* buffer_handler);

// this function is used within the thread functions to receive a new buffer
BGAPI2::Buffer* PullBufferFromDoubleBufferHandler(DoubleBufferHandler* buffer_handler);

// this function is used within the thread functions to return a buffer
void FreeBufferToDoubleBufferHandler(DoubleBufferHandler* buffer_handler, BGAPI2::Buffer* buffer);

// structure with the buffer object and a reference counter
// the reference counter is incremented with every call to PullBufferFromBufferModule
// and decremented with every call to FreeBufferToBufferModule
// if the reference counter reaches 0 the buffer will return to BGAPI
// the new data flag is available for all application threads
// this flags makes sure that one application thread of one camera process a camera buffer only once
struct CameraBuffer{
    CameraBuffer() {
        buffer_ = nullptr;
        ref_counter_ = 0;
    }
    std::mutex lock_;
    BGAPI2::Buffer* buffer_;
    std::atomic<int> ref_counter_;
    std::map<std::thread::id, std::atomic<bool>> new_data;
};
// this map manages all reference counter and buffer objects for each camera
std::map<DoubleBufferHandler*, CameraBuffer> current_buffer;

// result of calculation thread stored in simple string vector map
std::map<DoubleBufferHandler*, std::vector<std::string>> calculation_result;

// mutex to synchronize the access to the calculation result
std::mutex calculation_result_lock;

// ---------------
// helper function, used to reduce the length of some functions
// ---------------

// initialize buffer structure, which is used for buffer and data exchange between
// the main and application threads
void InitCurrentBufferStruct(const CameraManager * camera_manager, std::list<std::thread::id> thread_id_list);

// waits until all connected cameras are streaming
// prints the streaming status, camera name and serial number to console
void WaitUntilStreamingIsActive(CameraManager * camera_manager);

// function used in automatic mode to wait for all camera buffer
int WaitOfImages(CameraManager * camera_manager, unsigned int number_of_images);

// returnd the current system time as string in the format Day Month Data hh:mm:ss Year
std::string GetCurrentTime();

// used to convert the command line input to an numeric value
bool ConvertToNumericValue(std::string input,  uint32_t* const output_value);

// the following functions creating a string vector with header information to be used for a formatted output
void ObtainCameraNameHeader(std::vector<std::string> * data);
void ObtainCameraStatusHeader(std::vector<std::string> * data);
void ObtainStreamStatusHeader(std::vector<std::string> * data, bool use_resend_information);
void ObtainBufferStatusHeader(std::vector<std::string> * data);
void ObtainCalculationStatusHeader(std::vector<std::string> * data);

// the following functions creating a string vector with specific status information to be used for a formatted output
void ObtainCameraName(Camera* camera, std::vector<std::string>* data);
void ObtainCameraStatus(Camera* camera, std::vector<std::string>* data);
void ObtainStreamStatus(Camera* camera, std::vector<std::string>* data, bool use_resend_information);
void ObtainBufferStatus(Camera* camera, std::vector<std::string>* data);
void ObtainCalculationStatus(Camera * camera, std::vector<std::string>* data);

// the following functiona are helpers of the formatted output
void CreateEmptyBufferStatus(std::vector<std::string>* data);
void PrepareBufferStatus(std::vector<std::string>* data, BufferInformation* buffer_info);
unsigned int GetMaxStringLengthOfVector(std::vector<std::string>* string_list);
void PrintToConsole(std::vector<std::vector<std::string>>* status_matrix);

// this function prints a string matrix as a table to console
void PrintToConsole(std::vector<std::vector<std::string>>* status_matrix);

// theses functions creating several system time variants based on a time_t struct
time_t GenerateSystemClockStruct(bo_int64 hosttimestamp_nsec, std::string *host_fraction);
std::string GenerateSystemClockDate(time_t time_struct);
std::string GenerateSystemClockTime(time_t time_struct, std::string host_fraction);
std::string GenerateSystemClockTimeZone(time_t time_struct);

// ---------------
// example application functions used by the application threads
// ---------------

// logging output performed by the status thread
void PrintLoggingInformation(CameraManager * camera_manager);

// status output performed by the status thread
void PrintStatus(CameraManager * camera_manager);

// perform a mean value calculation used by the calculation thread
void DoCalculation(Camera * camera);

// -------------
// main routine of the example
// -------------

// this example shows the handling with several cameras and several tasks (threads) in parallel
// the program is running in a main loop which offers a menu with several commands
// the following commands are supported
// - capture images (capture command)
// - set exposure time (feature command)
// - camera status (status command)
// - perform image calculation and print the result (calculation command)
// all the commands running asynchronous to the main routine
// the status command and the calculation command are part of the main program
// the capture command and the feature command are running in the CameraManager class
// the program implements two application threads
// - status thread, is responsible for the status command
// - calculation thread, is responsible for the calculation command
// the program implements three worker threads
// - acquisition thread, is responsible for the capture command
// - feature thread, is responsible for the feature command
// - initialization thread, is responsible for searching of connected cameras
// ----
// at first the program starts the search for connected cameras by starting the initialisation thread
// it is not absolutely necessary to start the camera search in a separate thread but the example
// just show that it is possible to do so
// the found cameras will be opened and configured to work with
// the example starts the application threads and the worker threads
// the user has to select a command
// ----
// the example demonstrates the multi thread use case to give a good starting point for developing
// complex and high-performance applications
// this example shows the following concepts
// - two application threads sharing the same camera buffer
// - synchronisation of application threads
// - fast way to exchange camera buffer and information between application threads and the acquisition thread
// - a double buffer strategy for buffer exchange
int main(int argc, char* /*argv*/[]) {
    int return_code = 0;
    std::cout << "+---------------------------------------------+" << std::endl;
    std::cout << "| 019_MultipleCameraMultipleThread.cpp        |" << std::endl;
    std::cout << "+---------------------------------------------+" << std::endl;
    std::cout << std::endl;

    // instantiate the camera manager
    CameraManager camera_manager;

    // instantiate the buffer handler
    DoubleBufferHandler double_buffer;

    // instantiate the application threads: status thread and calculation thread
    std::thread status_thread;
    std::thread calculation_thread;

    std::cout << "program is looking for connected cameras.... " << std::endl;

    // configure the camera manager to work the double buffer handler
    // at this point it is possible to implement other buffer exchange strategies
    camera_manager.Start(double_buffer);

    // check, if everything is correct initialized and at least one camera was found
    if (camera_manager.IsInitOK()) {
        std::cout << "all connected cameras are successfully initialized and opened" << std::endl;
    } else {
        std::cout << "problem while initializing the BGAPI, program finished." << std::endl;
        return_code = 1;
    }
    if (camera_manager.NumberOfCameras() == 0) {
        std::cout << "no cameras connected, program finished." << std::endl;
        return_code = 1;
    }

    // print out some logging information, if there are some after initialization
    // and abort the program, if something goes wrong
    std::cout << camera_manager.LoggingMessages() << std::endl;

    if (return_code == 1) {
        camera_manager.Stop();
        return return_code;
    }

    // start the application threads
    // the threads needed a thread routine and access to the connected cameras
    status_thread = std::thread(StatusThreadRoutine, &camera_manager);
    calculation_thread = std::thread(CalculationThreadRoutine, &camera_manager);

    // initialize buffer structure, which is used for buffer and data exchange between
    // the main and application threads
    std::list<std::thread::id> thread_id_list;
    thread_id_list.push_back(status_thread.get_id());
    thread_id_list.push_back(calculation_thread.get_id());
    InitCurrentBufferStruct(&camera_manager, thread_id_list);

    // start and configure all connected cameras
    camera_manager.StartCameras();

    // the program can also be controlled automatically, without user input
    bool auto_flag = argc > 1;
    // if not used in automatic mode the user has to activate the desired command manually
    std::string user_input_string = "2";
    // the number of images to be captured
    unsigned int number_of_images = 100;
    // invalid marker
    const uint32_t invalid_input = 100;
    // variable to store the user input
    uint32_t user_input_num = invalid_input;
    // main menu of the main thread
    do {
        bool invalid_input_flag = false;
        std::cout << "+----------------------------------+" << std::endl;
        std::cout << "1 - Exit" << std::endl;
        std::cout << "2 - Capture images" << std::endl;
        std::cout << "3 - Set exposure time (micro sec)" << std::endl;
        std::cout << "4 - Print camera information" << std::endl;
        std::cout << "5 - " <<
            (camera_streaming_information ? "Deactivate" : "Activate") <<
            " information about the camera stream when perform command 4" << std::endl;
        std::cout << "6 - " <<
            (buffer_information ? "Deactivate" : "Activate") <<
            " information about the latest captured image when perform command 4" << std::endl;
        std::cout << "7 - " <<
            (command_calculation ? "Deactivate" : "Activate") <<
            " calculation of mean gray value" << std::endl;
        std::cout << "8 - Print trace information" << std::endl;
        std::cout << "9 - " << (disable_heartbeat ? "Enable" : "Disable") << " Heartbeat (for GEV only)" << std::endl;
        std::cout << "+----------------------------------+" << std::endl;
        std::cout << "Make your choice: ";
        // the automatic mode starts always and only the capture command
        if (!auto_flag) {
            std::getline(std::cin, user_input_string);
        }
        if (ConvertToNumericValue(user_input_string, &user_input_num)) {
            switch (user_input_num) {
            case 1:
                break;
            case 2:
                if (!auto_flag) {
                    std::cout << "Enter number of images you want to capture: ";
                    std::getline(std::cin, user_input_string);
                    if (!ConvertToNumericValue(user_input_string, &number_of_images)) {
                        number_of_images = 0;
                        invalid_input_flag = true;
                        break;
                    }
                } else {
                    std::cout << "Start image capture for " << number_of_images << " images" << std::endl;
                }
                if (number_of_images > 0) {
                    // start the capture command and inform the camera manager about the number of images to capture
                    // this command is not blocking and the user can start a new command
                    // directly after finishing this loop cycle
                    StartCaptureCommand(&camera_manager, number_of_images);
                    // block if the program starts in automatic mode until
                    // the number of captures images of all running cameras is reached
                    // or abort, if the number of captures images doesn't change within 1 sec
                    if (auto_flag) {
                        return_code = WaitOfImages(&camera_manager, number_of_images);
                        // print the status after finishing image capture
                        user_input_string = "4";
                    } else {
                        WaitUntilStreamingIsActive(&camera_manager);
                    }
                }
                break;
            case 3:
            {
                uint32_t exposure_time = 0;
                std::cout << "Enter new exposure time: ";
                std::getline(std::cin, user_input_string);
                if (ConvertToNumericValue(user_input_string, &exposure_time)) {
                    // start the feature command in the camera manager
                    // this command is not blocking and the user can start a new command
                    // directly after finishing this loop cycle
                    StartFeatureCommandExposure(&camera_manager, static_cast<double>(exposure_time));
                    std::cout << "Exposure time successfully written to cameras."
                        << " Please press 4 to print current exposure time." << std::endl;
                } else {
                    invalid_input_flag = true;
                }
                break;
            }
            case 4:
                // start the status command
                command_status = true;
                // this case branch blocks and waits until the command operation is finished, it makes sure
                // that the status thread finished the console output before starting a new command
                do {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                } while (command_status);
                if (auto_flag) {
                    user_input_string = "1";
                }
                break;
            case 5:
                // switch output of camera streaming information
                camera_streaming_information = !camera_streaming_information;
                break;
            case 6:
                // switch output of buffer information
                buffer_information = !buffer_information;
                break;
            case 7:
                // start / stop the calculation command
                command_calculation = !command_calculation;
                break;
            case 8:
                // activate printing logging information
                logging_information = true;
                // start the status command
                command_status = true;
                // this case branch blocks and waits until the command operation is finished, it makes sure
                // that the status thread finished the console output before starting a new command
                do {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                } while (command_status);
                logging_information = false;
                break;
            case 9:
                set_heartbeat = true;
                // disable heartbeat
                disable_heartbeat = !disable_heartbeat;
                // start the status command
                command_status = true;
                // this case branch blocks and waits until the command operation is finished, it makes sure
                // that the status thread finished the console output before starting a new command
                do {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                } while (command_status);
                set_heartbeat = false;
                break;
            default:
                invalid_input_flag = true;
            }
        } else {
            invalid_input_flag = true;
        }

        if (invalid_input_flag) {
            std::cout << "invalid input" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    } while (user_input_num != 1);

    // set the cancel flag of status thread
    finish_console_threads = true;

    // and wait until status thread has finished
    status_thread.join();

    // wait until calculation thread has finished
    calculation_thread.join();

    // free all BGAPI resources and worker threads
    camera_manager.StopCameras();
    camera_manager.Stop();

    // print out all remaining logging information, if any are available
    std::cout << camera_manager.LoggingMessages() << std::endl;

    std::cout << "019_MultipleCameraMultipleThread finished with returncode: " << return_code << std::endl;
    return return_code;
}


// -------------
// synchronized buffer access between the application threads
// the function PullBufferFromDoubleBufferHandler receive a new buffer from the camera manager,
// if the reference counter is 0
// each further call increases the reference counter
// every call to FreeBufferToDoubleBufferHandler decreases the reference counter
// if the reference counter is 0 the buffer is returned to the camera manager
// this makes sure that all application threads are working on the same camera buffer
// a new camera buffer is only used if all application threads have returned current camera buffer
// the new data flag inform all threads about receiving a new buffer
// -------------

// this function sets the new data flag for all connected cameras to true, if the buffer_ pointer is nullptr,
// that means if all application threads have returned their camera buffer
// and a new camera buffer is served by the buffer handler
bool HasDoubleBufferHandlerNewData(DoubleBufferHandler* buffer_handler) {
    std::lock_guard<std::mutex> lock(current_buffer[buffer_handler].lock_);
    if (current_buffer[buffer_handler].buffer_ == nullptr) {
        if (buffer_handler->HasNewData()) {
            std::map<std::thread::id, std::atomic<bool>>::iterator it = current_buffer[buffer_handler].new_data.begin();
            for (; it != current_buffer[buffer_handler].new_data.end(); it++) {
                (*it).second = true;
            }
        }
    }
    return current_buffer[buffer_handler].new_data[std::this_thread::get_id()];
}

// the function PullBufferFromDoubleBufferHandler uses a mutex to synchronize
// multiple calls to this function and to function FreeBufferToDoubleBufferHandler
// the mutex is entered, when this function is called and when the lock is not
// entered from FreeBufferToDoubleBufferHandler, in this case the current thread
// stops execution on the mutex object until FreeBufferToDoubleBufferHandler was left
BGAPI2::Buffer* PullBufferFromDoubleBufferHandler(DoubleBufferHandler* buffer_handler) {
    std::lock_guard<std::mutex> lock(current_buffer[buffer_handler].lock_);
    if (current_buffer[buffer_handler].buffer_ == nullptr) {
        current_buffer[buffer_handler].buffer_ = buffer_handler->PullBuffer();
    }
    if (current_buffer[buffer_handler].buffer_ != nullptr) {
        current_buffer[buffer_handler].ref_counter_++;
    }
    current_buffer[buffer_handler].new_data[std::this_thread::get_id()] = false;
    return current_buffer[buffer_handler].buffer_;
}

// vise versa to function PullBufferFromDoubleBufferHandler
void FreeBufferToDoubleBufferHandler(DoubleBufferHandler* buffer_handler, BGAPI2::Buffer* buffer) {
    std::lock_guard<std::mutex> lock(current_buffer[buffer_handler].lock_);
    if (buffer == current_buffer[buffer_handler].buffer_) {
        if (current_buffer[buffer_handler].ref_counter_ > 0) {
            current_buffer[buffer_handler].ref_counter_--;
        }
        if (current_buffer[buffer_handler].ref_counter_ == 0) {
            buffer_handler->FreeBuffer(buffer);
            current_buffer[buffer_handler].buffer_ = nullptr;
        }
    }
}

// -------------
// thread routines of application threads
// -------------

// the StatusThreadRoutine is left and the status thread finished, if the finish flag was set
// the StatusThreadRoutine starts working if the status command was activated from within the main loop
// the StatusThreadRoutine works with polling to detect an activated status command
void StatusThreadRoutine(CameraManager* camera_manager) {
    // abort condition of the thread routine
    while (!finish_console_threads) {
        // check if the user activated the status command
        if (command_status) {
            if (set_heartbeat) {
                camera_manager->SetHeartbeatDisable(disable_heartbeat);
            } else {
                if (!logging_information) {
                    // print current time
                    std::cout << std::endl << "camera information recorded at "
                        << GetCurrentTime() << ":" << std::endl;
                    // print status to console
                    PrintStatus(camera_manager);
                } else {
                    // print the latest logging messages to console
                    PrintLoggingInformation(camera_manager);
                }
            }
            // status command is now finished
            command_status = false;
        } else {
            // just wait until status command become active
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

// the CalculationThreadRoutine works with polling to detect an activated calculation command
// the CalculationThreadRoutine is left and the calculation thread is finished, if the finish flag was set
// the CalculationThreadRoutine starts working if the calculation command was activated from within the main loop
void CalculationThreadRoutine(CameraManager* camera_manager) {
    // the main loop of the thread, leaving this loop means finishing the thread
    // wait with the join function in the main thread until this loop was left
    while (!finish_console_threads) {
        // check if the user activated the status command
        if (command_calculation) {
            // perform calculation
            const std::vector<Camera*>* camera_vector = camera_manager->GetCameraVector();
            // iterate over all cameras and print the status messages to console
            for (std::vector<Camera*>::const_iterator camera_iter = camera_vector->begin();
                camera_iter != camera_vector->end();
                camera_iter++) {
                DoCalculation(*camera_iter);
            }
            // wait a long time before proceeding, to show that more buffers are captured
            // then the calculation thread can process
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } else {
            // just wait until status command become active
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

// initialize the new_data information of the buffer struct with 'true' for all connected cameras
void InitCurrentBufferStruct(const CameraManager* camera_manager, std::list<std::thread::id> thread_id_list) {
    const std::vector<Camera*>* camera_vector = camera_manager->GetCameraVector();
    for (std::vector<Camera*>::const_iterator camera_iter = camera_vector->begin();
        camera_iter != camera_vector->end();
        camera_iter++) {
        for (std::list<std::thread::id>::iterator iter = thread_id_list.begin(); iter != thread_id_list.end(); iter++) {
            current_buffer[&(*camera_iter)->buffer_handler_].new_data[*iter] = true;
        }
    }
}

// waits until all connected cameras are streaming
// and print the streaming status to console
void WaitUntilStreamingIsActive(CameraManager * camera_manager) {
    bool all_cameras_active = true;
    unsigned int repeats = 10;
    unsigned int repeat_counter = 0;
    std::stringstream cameras_streaming_status;
    do {
        cameras_streaming_status.str("");
        all_cameras_active = true;
        const std::vector<Camera*>* camera_vector = camera_manager->GetCameraVector();
        for (std::vector<Camera*>::const_iterator camera_iter = camera_vector->begin();
            camera_iter != camera_vector->end();
            camera_iter++) {
            if ((*camera_iter)->capture_active_) {
                cameras_streaming_status << "Camera " << (*camera_iter)->GetBGAPIPointer()->GetModel()
                    << "(" << (*camera_iter)->GetBGAPIPointer()->GetSerialNumber() << ")"
                    << " successfully started" << std::endl;
            } else {
                cameras_streaming_status << "Camera " << (*camera_iter)->GetBGAPIPointer()->GetModel()
                    << "(" << (*camera_iter)->GetBGAPIPointer()->GetSerialNumber() << ")"
                    << " not started" << std::endl;
                all_cameras_active = false;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (!all_cameras_active && repeat_counter++ < repeats);
    if (all_cameras_active) {
        std::cout << "Capture command successfully started. Please hit 4 to print camera status." << std::endl;
    } else {
        std::cout << "Capture command not successfully. Not all camera are started correctly." << std::endl;
    }
    std::cout << cameras_streaming_status.str() << std::endl;
}

// this function waits until the expected number of images are captured or aborts
// if the time out time is reached
int WaitOfImages(CameraManager * camera_manager, unsigned int number_of_images) {
    int return_code = 0;
    unsigned int timeout_count = 0;
    unsigned int last_numofimages = 0;
    unsigned int captured_numofimages = 0;
    do {
        captured_numofimages = camera_manager->CapturedImages();
        if (captured_numofimages != last_numofimages) {
            last_numofimages = captured_numofimages;
            timeout_count = 0;
            return_code = 0;
        } else {
            timeout_count++;
            return_code = 1;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    } while (captured_numofimages < number_of_images && timeout_count < 100);
    return return_code;
}

std::string GetCurrentTime() {
    std::chrono::time_point<std::chrono::system_clock> system_now = std::chrono::system_clock::now();
    std::time_t time = std::chrono::system_clock::to_time_t(system_now);
    std::stringstream current_date;
    struct tm * timeinfo = std::localtime(&time);
    char timebuffer[80] = { 0 };
    strftime(timebuffer, 80, "%c", timeinfo);
    current_date << timebuffer;
    return current_date.str();
}

// convert a string to a numeric value
bool ConvertToNumericValue(std::string input,  uint32_t* const output_value) {
    if (!output_value) {
        return false;
    }
    if (input.size() == 0) {
        return false;
    }
    for (auto it = input.begin(); it != input.end(); ++it) {
        if (!std::isdigit(*it)) {
            return false;
        }
    }
    (*output_value) = std::stoi(input);
    return true;
}

void StartFeatureCommandExposure(CameraManager * camera_manager, double exposure_time) {
    const std::vector<Camera*>* camera_vector = camera_manager->GetCameraVector();
    for (std::vector<Camera*>::const_iterator camera_iter = camera_vector->begin();
        camera_iter != camera_vector->end();
        camera_iter++) {
        (*camera_iter)->StartFeatureCommandExposure(exposure_time);
    }
}

void StartCaptureCommand(CameraManager * camera_manager, unsigned int images) {
    const std::vector<Camera*>* camera_vector = camera_manager->GetCameraVector();
    for (std::vector<Camera*>::const_iterator camera_iter = camera_vector->begin();
        camera_iter != camera_vector->end();
        camera_iter++) {
        (*camera_iter)->StartCaptureCommand(images);
    }
}

void PrintLoggingInformation(CameraManager * camera_manager) {
    std::cout << std::endl;
    std::cout << "Logging information"
        << std::endl << "-------------------" << std::endl;
    std::string log_string = camera_manager->LoggingMessages();
    if (log_string.size() > 0) {
        std::cout << log_string << std::endl;
    } else {
        std::cout << "No messages present." << std::endl;
    }
    std::cout << std::endl;
}

// PrintStatus collect several status information depending if they are enabled or not
// and print this information to the console
void PrintStatus(CameraManager * camera_manager) {
    std::vector<std::vector<std::string>> status_matrix;
    std::vector<std::string> header_arr;
    bool use_resend_info = false;

    ObtainCameraNameHeader(&header_arr);
    ObtainCameraStatusHeader(&header_arr);
    if (camera_streaming_information) {
        use_resend_info = camera_manager->ResendStatisticSupported();
        ObtainStreamStatusHeader(&header_arr, use_resend_info);
    }
    if (buffer_information) {
        ObtainBufferStatusHeader(&header_arr);
    }
    if (command_calculation) {
        ObtainCalculationStatusHeader(&header_arr);
    }
    status_matrix.push_back(header_arr);

    const std::vector<Camera*>* camera_vector = camera_manager->GetCameraVector();
    for (std::vector<Camera*>::const_iterator camera_iter = camera_vector->begin();
        camera_iter != camera_vector->end();
        camera_iter++) {
        std::vector<std::string> data_arr;
        ObtainCameraName(*camera_iter, &data_arr);
        ObtainCameraStatus(*camera_iter, &data_arr);
        if (camera_streaming_information) {
            ObtainStreamStatus(*camera_iter, &data_arr, use_resend_info);
        }
        if (buffer_information) {
            ObtainBufferStatus(*camera_iter, &data_arr);
        }
        if (command_calculation) {
            ObtainCalculationStatus(*camera_iter, &data_arr);
        }
        status_matrix.push_back(data_arr);
    }
    PrintToConsole(&status_matrix);
}

// the DoCalculation receive one camera buffer and perform the calculation on the image data
// the DoCalculation use the synchronisation functions for a synchronized buffer access
// the DoCalculation works only once on a camera buffer (see HasDoubleBufferHandlerNewData)
// the DoCalculation writes the calculation result in a string vector
void DoCalculation(Camera * camera) {
    BGAPI2::String camera_name = camera->GetBGAPIPointer()->GetModel();
    std::stringstream convert_stream;
    std::vector<std::string> result_vector;
    if (HasDoubleBufferHandlerNewData(&camera->buffer_handler_)) {
        BGAPI2::Buffer * buffer = PullBufferFromDoubleBufferHandler(&camera->buffer_handler_);
        if (buffer) {
            BufferInformation * buffer_info = reinterpret_cast<BufferInformation*>(buffer->GetUserObj());
            if (!buffer->GetIsIncomplete()) {
                try {
                    unsigned char* image_buffer = static_cast<unsigned char*>(buffer->GetMemPtr());
                    int width = static_cast<int>(buffer->GetWidth());
                    int height = static_cast<int>(buffer->GetHeight());
                    int pixel_step_x = static_cast<int>(width / 200);
                    int pixel_step_y = static_cast<int>((pixel_step_x * height) / width);
                    int pixel_counter = 0;
                    double pixel_sum = 0.0;
                    for (int h = 0; h < height; h += pixel_step_y) {
                        for (int w = 0; w < width; w += pixel_step_x) {
                            pixel_sum += image_buffer[h*width + w];
                            pixel_counter++;
                        }
                    }
                    double average = pixel_sum / pixel_counter;
                    result_vector.push_back("ok");
                    convert_stream << buffer_info->frameid;
                    result_vector.push_back(convert_stream.str());
                    convert_stream.str("");
                    convert_stream << std::fixed << std::setprecision(3) << average;
                    result_vector.push_back(convert_stream.str());
                    std::lock_guard<std::mutex> lock(calculation_result_lock);
                    calculation_result[&camera->buffer_handler_] = result_vector;
                }
                catch (BGAPI2::Exceptions::IException &ex) {
                    (void)(ex);
                    result_vector.push_back("exception");
                    convert_stream << buffer_info->frameid;
                    result_vector.push_back(convert_stream.str());
                    result_vector.push_back("-");
                    std::lock_guard<std::mutex> lock(calculation_result_lock);
                    calculation_result[&camera->buffer_handler_] = result_vector;
                }
            } else {
                result_vector.push_back("IncompleteImage");
                result_vector.push_back("-");
                result_vector.push_back("-");
                std::lock_guard<std::mutex> lock(calculation_result_lock);
                calculation_result[&camera->buffer_handler_] = result_vector;
            }
            FreeBufferToDoubleBufferHandler(&camera->buffer_handler_, buffer);
        } else {
            result_vector.push_back("NoImageCaptured");
            result_vector.push_back("-");
            result_vector.push_back("-");
            std::lock_guard<std::mutex> lock(calculation_result_lock);
            calculation_result[&camera->buffer_handler_] = result_vector;
        }
    }
}

void ObtainCameraNameHeader(std::vector<std::string> * data) {
    data->push_back(" ");
    data->push_back("+----------------+");
}

void ObtainCameraStatusHeader(std::vector<std::string> * data) {
    data->push_back("<Heading>Camera information");
    data->push_back("+----------------+");
    data->push_back("ExposureTime");
    data->push_back("ROI");
    data->push_back("FrameCounter");
    data->push_back("Timestamp");
    data->push_back("Heartbeat");
    data->push_back("<Heading>");
}

void ObtainStreamStatusHeader(std::vector<std::string> * data, bool use_resend_information) {
    data->push_back("<Heading>Stream information");
    data->push_back("+----------------+");
    data->push_back("CaptureActive");
    data->push_back("ImagesCaptured");
    data->push_back("ImagesIncomplete");
    if (use_resend_information) {
        data->push_back("Resends");
        data->push_back("SingleResends");
        data->push_back("RangeResends");
        data->push_back("ReceivedPackets");
    }
    data->push_back("<Heading>");
}

void ObtainBufferStatusHeader(std::vector<std::string> * data) {
    data->push_back("<Heading>Buffer information");
    data->push_back("+----------------+");
    data->push_back("BufferId");
    data->push_back("FrameId");
    data->push_back("SensorId");
    data->push_back("IsIncomplete");

    data->push_back("<Heading>");
    data->push_back("<Heading>Time information of the image obtained at different points in time");
    data->push_back("<Heading>shows the difference between two consecutive images (Diff)");
    data->push_back("<Heading>and short statistic (Min, Max, Average)");
    data->push_back("<Heading>");

    data->push_back("<Heading>time stamp at the time of acquisition in the camera");
    data->push_back("<Heading>");
    data->push_back("Timestamp");
    data->push_back("TimestampDiff");
    data->push_back("TimestampDiffMin");
    data->push_back("TimestampDiffAve");
    data->push_back("TimestampDiffMax");
    data->push_back("<Heading>");

    data->push_back(
        "<Heading>time information at the time when receiving the first data block of the image on the host");
    data->push_back("<Heading>");
    data->push_back("Date");
    data->push_back("Time");
    data->push_back("TimeZone");
    data->push_back("TimeDiff");
    data->push_back("TimeDiffMin");
    data->push_back("TimeDiffAve");
    data->push_back("TimeDiffMax");
    data->push_back("<Heading>");

    data->push_back("<Heading>time information at the time when the function GetFilledBuffer receive the image");
    data->push_back("<Heading>");
    data->push_back("Date");
    data->push_back("Time");
    data->push_back("TimeZone");
    data->push_back("TimeDiff");
    data->push_back("TimeDiffMin");
    data->push_back("TimeDiffAve");
    data->push_back("TimeDiffMax");
    data->push_back("<Heading>");
}

void ObtainCalculationStatusHeader(std::vector<std::string> * data) {
    data->push_back("<Heading>Calculation information");
    data->push_back("+----------------+");
    data->push_back("Status");
    data->push_back("ProcessedFrameId");
    data->push_back("MeanValue");
    data->push_back("<Heading>");
}

void ObtainCameraName(Camera* camera, std::vector<std::string>* data) {
    data->push_back(camera->GetName());
    data->push_back("+------+");
}

void ObtainCameraStatus(Camera* camera, std::vector<std::string>* data) {
    std::stringstream convert_stream;

    data->push_back("<Heading>");
    data->push_back("+------+");

    double exposure_time = 0.0f;
    convert_stream.str("");
    if (camera->GetExposureTime(&exposure_time)) {
        convert_stream << exposure_time;
    } else {
        convert_stream << "na";
    }
    data->push_back(convert_stream.str());

    uint64_t roi = 0;
    convert_stream.str("");
    if (camera->GetRoiOffsetX(&roi)) {
        convert_stream << roi;
    } else {
        convert_stream << "na";
    }
    convert_stream << " ";
    if (camera->GetRoiOffsetY(&roi)) {
        convert_stream << roi;
    } else {
        convert_stream << "na";
    }
    convert_stream << " ";
    if (camera->GetRoiWidth(&roi)) {
        convert_stream << roi;
    } else {
        convert_stream << "na";
    }
    convert_stream << " ";
    if (camera->GetRoiHeight(&roi)) {
        convert_stream << roi;
    } else {
        convert_stream << "na";
    }
    data->push_back(convert_stream.str());
    
    uint64_t frame_counter = 0;
    convert_stream.str("");
    if (camera->GetFrameCounter(&frame_counter)) {
        convert_stream << frame_counter;
    } else {
        convert_stream << "na";
    }
    data->push_back(convert_stream.str());

    uint64_t current_timestamp = 0;
    convert_stream.str("");
    if (camera->GetTimeStamp(&current_timestamp)) {
        uint64_t ts_freq = 0;
        camera->GetTimeStampFrequency(&ts_freq);
        double camrea_ts = ts_freq != 0 ? static_cast<double>(current_timestamp) / ts_freq : 0;
        convert_stream << std::fixed << std::setprecision(3)
            << camrea_ts << " s";
    } else {
        convert_stream << "na";
    }
    data->push_back(convert_stream.str());

    bool heart_beat_disable = true;
    convert_stream.str("");
    if (camera->GetHeartbeatDisable(&heart_beat_disable)) {
        convert_stream << (heart_beat_disable ? "false" : "true");
    } else {
        convert_stream << "na";
    }
    data->push_back(convert_stream.str());

    data->push_back("<Heading>");
}

void ObtainStreamStatus(Camera* camera, std::vector<std::string>* data, bool use_resend_information) {
    std::stringstream convert_stream;

    data->push_back("<Heading>");
    data->push_back("+------+");

    if (camera->CaptureActive()) {
        data->push_back("true");
    } else {
        data->push_back("false");
    }
    convert_stream.str("");
    convert_stream << camera->GetCapturedImages();
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << camera->GetIncompleteImages();
    data->push_back(convert_stream.str());
    if (use_resend_information) {
        if (camera->IsResendStatisticSupported()) {
            convert_stream.str("");
            convert_stream << camera->GetResendRequests();
            data->push_back(convert_stream.str());
            convert_stream.str("");
            convert_stream << camera->GetResendSingleRequests();
            data->push_back(convert_stream.str());
            convert_stream.str("");
            convert_stream << camera->GetResendRangeRequests();
            data->push_back(convert_stream.str());
            convert_stream.str("");
            convert_stream << camera->GetReceivedPackets();
            data->push_back(convert_stream.str());
        } else {
            data->push_back("NotSupported");
            data->push_back("NotSupported");
            data->push_back("NotSupported");
            data->push_back("NotSupported");
        }
    }
    data->push_back("<Heading>");
}

void CreateEmptyBufferStatus(std::vector<std::string>* data) {
    data->push_back("<Heading>");
    data->push_back("+------+");
    data->push_back("NoImageCaptured");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("<Heading>");

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("<Heading>");

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("<Heading>");

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("-");
    data->push_back("<Heading>");
}

void PrepareBufferStatus(std::vector<std::string>* data, BufferInformation* buffer_info) {
    data->push_back("<Heading>");
    data->push_back("+------+");
    std::stringstream convert_stream;
    if (buffer_info->bufferid.size() > 8) {
        convert_stream << buffer_info->bufferid.substr(buffer_info->bufferid.size() - 8);
    } else {
        convert_stream << buffer_info->bufferid;
    }

    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << buffer_info->frameid;
    data->push_back(convert_stream.str());
    convert_stream.str("");
    if (buffer_info->supports_frameid_sensor) {
        convert_stream << buffer_info->frameid_sensor;
    } else {
        convert_stream << "NotSupported";
    }
    data->push_back(convert_stream.str());
    if (buffer_info->is_imcomplete) {
        data->push_back("true");
    } else {
        data->push_back("false");
    }

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("<Heading>");
    data->push_back("<Heading>");

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    convert_stream.str("");
    double camrea_ts = buffer_info->camera_buffer_ts_freq != 0 ?
        static_cast<double>(buffer_info->camera_buffer_ts) / buffer_info->camera_buffer_ts_freq : 0;
    convert_stream << std::fixed << std::setprecision(3)
        << camrea_ts << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->camera_buffer_ts_diff / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->camera_buffer_ts_diff_min / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->camera_buffer_ts_diff_ave / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->camera_buffer_ts_diff_max / 1000.0 << " s";
    data->push_back(convert_stream.str());
    data->push_back("<Heading>");

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    std::string fraction;
    time_t time_struct = GenerateSystemClockStruct(buffer_info->host_buffer_ts, &fraction);
    convert_stream.str("");
    convert_stream << GenerateSystemClockDate(time_struct);
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << GenerateSystemClockTime(time_struct, fraction);
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << GenerateSystemClockTimeZone(time_struct);
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->host_buffer_ts_diff / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->host_buffer_ts_diff_min / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->host_buffer_ts_diff_ave / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->host_buffer_ts_diff_max / 1000.0 << " s";
    data->push_back(convert_stream.str());
    data->push_back("<Heading>");

    data->push_back("<Heading>");
    data->push_back("<Heading>");
    time_struct = GenerateSystemClockStruct(buffer_info->getfilled_buffer_ts, &fraction);
    convert_stream.str("");
    convert_stream << GenerateSystemClockDate(time_struct);
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << GenerateSystemClockTime(time_struct, fraction);
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << GenerateSystemClockTimeZone(time_struct);
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->getfilled_buffer_ts_diff / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->getfilled_buffer_ts_diff_min / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->getfilled_buffer_ts_diff_ave / 1000.0 << " s";
    data->push_back(convert_stream.str());
    convert_stream.str("");
    convert_stream << std::fixed << std::setprecision(3) << buffer_info->getfilled_buffer_ts_diff_max / 1000.0 << " s";
    data->push_back(convert_stream.str());
    data->push_back("<Heading>");
}

// the ObtainBufferStatus receive the oldest captured camera buffer with buffer information from the passed camera
// the ObtainBufferStatus use the synchronisation functions for a synchronized buffer access
// the ObtainBufferStatus works only once on the same camera buffer (see HasDoubleBufferHandlerNewData),
// a second call to HasDoubleBufferHandlerNewData returns false if no new image was captured
// in this case we switch our strategy (oldest image) and fetch buffer information from the latest (or last) image.
void ObtainBufferStatus(Camera* camera, std::vector<std::string>* data) {
    bool use_latest_bufferinfo = false;
    // check for a new camera buffer
    if (HasDoubleBufferHandlerNewData(&camera->buffer_handler_)) {
        // receive a camera buffer
        BGAPI2::Buffer * buffer = PullBufferFromDoubleBufferHandler(&camera->buffer_handler_);
        if (buffer) {
            BufferInformation * buffer_info = reinterpret_cast<BufferInformation*>(buffer->GetUserObj());
            PrepareBufferStatus(data, buffer_info);
            // return the camera buffer
            FreeBufferToDoubleBufferHandler(&camera->buffer_handler_, buffer);
        } else {
            use_latest_bufferinfo = true;
        }
    } else {
        use_latest_bufferinfo = true;
    }
    if (use_latest_bufferinfo) {
        BufferInformation latest_buffer_info;
        camera->GetLatestBufferInformation(&latest_buffer_info);
        if (latest_buffer_info.valid) {
            PrepareBufferStatus(data, &latest_buffer_info);
        } else {
            CreateEmptyBufferStatus(data);
        }
    }
}

void ObtainCalculationStatus(Camera * camera, std::vector<std::string>* data) {
    data->push_back("<Heading>");
    data->push_back("+------+");

    std::stringstream result_stream;
    if (calculation_result[&camera->buffer_handler_].size() == 0) {
        data->push_back("NoImageCaptured");
        data->push_back("-");
        data->push_back("-");
    } else {
        std::lock_guard<std::mutex> lock(calculation_result_lock);
        for (unsigned int idx = 0; idx < calculation_result[&camera->buffer_handler_].size(); idx++) {
            data->push_back(calculation_result[&camera->buffer_handler_][idx]);
        }
    }
    data->push_back("<Heading>");
}

// returns the length of the longest string in the passed list
unsigned int GetMaxStringLengthOfVector(std::vector<std::string>* string_list) {
    size_t string_length = 0;
    for (std::vector<std::string>::iterator iter = string_list->begin(); iter != string_list->end(); iter++) {
        // ignore the heading lines
        if ((*iter).find("<Heading>") == std::string::npos) {
            string_length = (*iter).size() > string_length ? (*iter).size() : string_length;
        }
    }
    return (unsigned int)string_length;
}

// this function prints the passed string matrix as table to console output
void PrintToConsole(std::vector<std::vector<std::string>>* status_matrix) {
    // use the first column to get the number of output_lines,
    // because we asume that all columns have the same size
    unsigned int output_lines = (unsigned int)status_matrix[0][0].size();

    // iterate over all columns to determine the width of each coloumn
    std::vector<unsigned int> column_width;
    for (unsigned int column_idx = 0; column_idx < status_matrix->size(); column_idx++) {
        column_width.push_back(GetMaxStringLengthOfVector(&(*status_matrix)[column_idx]));
    }

    std::cout << std::endl;
    for (unsigned int line_idx = 0; line_idx < output_lines; line_idx++) {
        for (unsigned int column_idx = 0; column_idx < status_matrix->size(); column_idx++) {
            std::vector<std::string> temp_vector;
            // genaerate special output of heading information in the first coloumn
            if (column_idx == 0) {
                if ((*status_matrix)[column_idx][line_idx].find("<Heading>") == std::string::npos) {
                    std::cout << " " << std::left << std::setw(column_width[column_idx])
                        << (*status_matrix)[column_idx][line_idx];
                } else {
                    std::cout << " " << std::left
                        << (*status_matrix)[column_idx][line_idx].substr(std::string("<Heading>").size());
                }
            } else {
                if ((*status_matrix)[column_idx][line_idx].find("<Heading>") == std::string::npos) {
                    std::cout << " " << std::right
                        << std::setw(column_width[column_idx]) << (*status_matrix)[column_idx][line_idx];
                }
            }
        }
        std::cout << std::endl;
    }
}

// this function creates a time_t struct from a passed timestamp
time_t GenerateSystemClockStruct(bo_int64 hosttimestamp_nsec, std::string *host_fraction) {
    std::stringstream systemclock_stream;
    std::chrono::steady_clock::time_point tp_steady =
        std::chrono::steady_clock::time_point(std::chrono::nanoseconds(hosttimestamp_nsec));

    // convert steady clock to system clock

    std::chrono::system_clock::time_point tp_system_host =
        std::chrono::system_clock::now() -
        std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::steady_clock::now() - tp_steady);

    // get seconds and microseconds fraction
    auto tp_system_host_seconds = std::chrono::time_point_cast<std::chrono::seconds>(tp_system_host);
    auto tp_system_host_fraction = tp_system_host - tp_system_host_seconds;

    // print the rest with nanoseconds accuracy
    // auto tp_system_host_nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(tp_system_host_fraction);
    // ssTimestamp_systemclock_us << std::setw(9) << std::setfill('0')
    //     << tp_system_host_nanoseconds.count() << std::setfill(' ');
    // print the rest with milliseconds accuracy
    auto tp_system_host_milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(tp_system_host_fraction);
    systemclock_stream << std::setw(3) << std::setfill('0')
        << tp_system_host_milliseconds.count() << std::setfill(' ');

    *host_fraction = systemclock_stream.str();

    time_t cnow = std::chrono::system_clock::to_time_t(tp_system_host);
    return cnow;
}

// this function creates a time string in the format YYYY-MM-DD
std::string GenerateSystemClockDate(time_t time_struct) {
    std::stringstream ssTimestamp_systemclock_us;
    struct tm * timeinfo = std::localtime(&time_struct);
    char timebuffer[80] = { 0 };
    strftime(timebuffer, 80, "%Y-%m-%d", timeinfo);
    ssTimestamp_systemclock_us << timebuffer;
    return ssTimestamp_systemclock_us.str();
}

// this function creates a time string in the format hh:mm:ss.fraction
std::string GenerateSystemClockTime(time_t time_struct, std::string host_fraction) {
    std::stringstream ssTimestamp_systemclock_us;
    struct tm * timeinfo = std::localtime(&time_struct);
    char timebuffer[80] = { 0 };
    strftime(timebuffer, 80, "%H:%M:%S", timeinfo);
    ssTimestamp_systemclock_us << timebuffer;
    ssTimestamp_systemclock_us << "." << host_fraction;
    return ssTimestamp_systemclock_us.str();
}

// this function creates a string with the TimeZone
std::string GenerateSystemClockTimeZone(time_t time_struct) {
    std::stringstream ssTimestamp_systemclock_us;
    struct tm * timeinfo = std::localtime(&time_struct);
    char timebuffer[80] = { 0 };
    strftime(timebuffer, 80, "%z", timeinfo);
    ssTimestamp_systemclock_us << timebuffer;
    return ssTimestamp_systemclock_us.str();
}
