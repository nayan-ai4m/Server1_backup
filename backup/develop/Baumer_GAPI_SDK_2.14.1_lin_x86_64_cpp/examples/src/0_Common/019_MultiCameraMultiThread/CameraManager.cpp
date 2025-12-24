/* Copyright 2019-2020 Baumer Optronic */
#include <iostream>
#include <sstream>
#include <limits>
#include <vector>
#include <iomanip>
#include <string>
#include "CameraManager.h"
#include "CameraTiming.h"
#include "DoubleBufferHandler.h"
#include "BufferInformation.h"

// this function initializes the Baumer GAPI SDK and
// searches for connected cameras
// all found cameras will be opened to work with
void InitThreadRoutine(CameraManager* camera_manager);

// this function handles the image capture command
// every camera has its own capture thread
void CaptureThreadRoutine(CameraManager* camera_manager, Camera * camera);

// this function handles the feature command (reading and writing features)
// this example shows only read and write to the ExposureTime feature
// every camera has its own feature thread
void FeatureThreadRoutine(CameraManager* camera_manager, Camera * camera);

CameraManager::CameraManager()
    : finish_worker_threads_(false)
    , init_ok_(true) {
}

CameraManager::~CameraManager() {
}

// this function starts the initialisation thread
// and blocks the execution until the thread is finished
void CameraManager::Start(DoubleBufferHandler buffer_handler) {
    buffer_handler_ = buffer_handler;
    init_thread = std::thread(InitThreadRoutine, this);
    init_thread.join();
}

// DeinitializeBGAPI is processed in the thread of the caller
// it is also possible to call DeinitializeBGAPI in a separate thread
void CameraManager::Stop() {
    DeinitializeBGAPI();
    cameras_.clear();
    init_ok_ = true;
}

size_t CameraManager::NumberOfCameras() const {
    return cameras_.size();
}

// initialize BGAPI buffer management is not performed in an extra thread
void CameraManager::StartCameras() {
    finish_worker_threads_ = false;
    for (std::vector<Camera*>::iterator camera_iter = cameras_.begin();
        camera_iter != cameras_.end();
        camera_iter++) {
        (*camera_iter)->InitializeBGAPIBufferManagement();
        // start the worker threads
        (*camera_iter)->capture_thread_ = std::thread(CaptureThreadRoutine, this, (*camera_iter));
        (*camera_iter)->feature_thread_ = std::thread(FeatureThreadRoutine, this, (*camera_iter));
    }
}

// this function informs all worker threads to exit themselves and waits with join() until the threads are finished
// if the thread is already finished before join() is called, the join() function doesn't wait
// and return immediately
// unlike initialization, deinitialization is not performed in an extra thread
void CameraManager::StopCameras() {
    // inform all worker threads to exit themselves
    finish_worker_threads_ = true;
    for (std::vector<Camera*>::iterator camera_iter = cameras_.begin();
        camera_iter != cameras_.end();
        camera_iter++) {
        // wait until the thread loop was left
        (*camera_iter)->capture_thread_.join();
        (*camera_iter)->feature_thread_.join();
        (*camera_iter)->DeinitializeBGAPIBufferManagement();
    }
}

// this function is called from the initialisation thread
void CameraManager::AddCamera(BGAPI2::Device* camera_pointer) {
    Camera * camera_obj = new CameraTiming(camera_pointer);
    camera_obj->buffer_handler_ = buffer_handler_;
    cameras_.push_back(camera_obj);
}

// to make sure that this function returns 0 if no image was captured
// the variable (*camera_iter)->number_of_captured_images_ will be reset to 0
// when the capture command starts (StartStreamingBGAPI)
// and if no camera is connected an additional check is included to set number_of_captured_images to 0
// to calculate the variable number_of_captured_images there is no need for a mutex,
// because this function performs only reads on (*camera_iter)->number_of_captured_images_
unsigned int CameraManager::CapturedImages() {
    unsigned int number_of_captured_images = std::numeric_limits<unsigned int>::max();
    for (std::vector<Camera*>::iterator camera_iter = cameras_.begin();
        camera_iter != cameras_.end();
        camera_iter++) {
        unsigned int current_device_images = (*camera_iter)->number_of_captured_images_;
        number_of_captured_images = current_device_images < number_of_captured_images ?
            current_device_images : number_of_captured_images;
    }
    if (number_of_captured_images == std::numeric_limits<unsigned int>::max()) {
        number_of_captured_images = 0;
    }
    return number_of_captured_images;
}

void CameraManager::SetInitError() {
    init_ok_ = false;
}

bool CameraManager::IsInitOK() const {
    return init_ok_;
}

// this function protects the logging_list_ against parallel access from function LoggingString
void CameraManager::AddLoggingMessage(std::string logging_message) {
    std::lock_guard<std::mutex> lock(logging_list_lock_);
    logging_list_.push_back(logging_message);
}

// this function protects the logging_list_ against parallel access from function LoggingStringAdd
std::string CameraManager::LoggingMessages() {
    std::lock_guard<std::mutex> lock(logging_list_lock_);
    std::stringstream logging_stream;
    while (logging_list_.size() > 0) {
        logging_stream << logging_list_.front() << std::endl;
        logging_list_.pop_front();
    }
    // camera specific
    for (std::vector<Camera*>::const_iterator camera_iter = cameras_.begin();
        camera_iter != cameras_.end();
        camera_iter++) {
        std::string log_string = (*camera_iter)->LoggingMessages();
        if (log_string.size() > 0) {
            logging_stream << log_string << std::endl;
        }
    }
    return logging_stream.str();
}

bool CameraManager::ResendStatisticSupported() {
    for (std::vector<Camera*>::const_iterator camera_iter = cameras_.begin();
        camera_iter != cameras_.end();
        camera_iter++) {
        if ((*camera_iter)->IsResendStatisticSupported()) {
            return true;
        }
    }
    return false;
}

void CameraManager::SetHeartbeatDisable(bool disable_heartbeat) {
    for (std::vector<Camera*>::const_iterator camera_iter = cameras_.begin();
        camera_iter != cameras_.end();
        camera_iter++) {
        (*camera_iter)->SetHeartbeatDisable(disable_heartbeat);
    }
}

// the function loads only one GenTL producer of type GEV and U3V at a time
// function has some special debug code when working with GigE cameras
// the heartbeat will be enabled disabled to allow debugging with losing the camera
void CameraManager::InitializeBGAPI() {
    bool device_found = false;
    bool gev_producer_found = false;
    bool u3v_producer_found = false;
    try {
        // get system list (list of GenTL producer)
        // and refresh the list
        BGAPI2::SystemList *system_list = BGAPI2::SystemList::GetInstance();
        system_list->Refresh();

        if (system_list->size() > 0) {
            AddLoggingMessage("GenTL producers loaded.");
        } else {
            AddLoggingMessage("No GenTL producers loaded.");
        }

        // use only the first system
        // remove the break keyword at the end of the loop to allow all systems
        for (BGAPI2::SystemList::iterator sys_iter = system_list->begin(); sys_iter != system_list->end(); sys_iter++) {
            BGAPI2::System *system_pointer = *sys_iter;
            BGAPI2::String tl_type = system_pointer->GetTLType();
            if (((tl_type == "GEV") && !gev_producer_found) ||
                ((tl_type == "U3V") && !u3v_producer_found)) {
                if (tl_type == "GEV") {
                    gev_producer_found = true;
                }
                if (tl_type == "U3V") {
                    u3v_producer_found = true;
                }
                AddLoggingMessage("  GenTL producer " + std::string(system_pointer->GetPathName()) + " found");
                AddLoggingMessage("    GenTL producer version is " + std::string(system_pointer->GetVersion()));
                system_pointer->Open();

                // get and refresh the interface list of the current system
                BGAPI2::InterfaceList *interface_list = system_pointer->GetInterfaces();
                interface_list->Refresh(100);

                // iterate over all interfaces
                for (BGAPI2::InterfaceList::iterator ifc_iter = interface_list->begin();
                    ifc_iter != interface_list->end();
                    ifc_iter++) {
                    // open the interface to work with
                    ifc_iter->Open();
                    // get and refresh the device list of the current interface
                    BGAPI2::DeviceList *device_list = ifc_iter->GetDevices();
                    device_list->Refresh(100);
                    if (device_list->size() > 0) {
                        // iterate and open all devices within the device list
                        for (BGAPI2::DeviceList::iterator dev_iter = device_list->begin();
                            dev_iter != device_list->end();
                            dev_iter++) {
                            BGAPI2::Device * camera_pointer = *dev_iter;
                            try {
                                // add the device pointer to the finder class
                                AddCamera(camera_pointer);
                                device_found = true;
                                AddLoggingMessage("    Device " + std::string(camera_pointer->GetModel()) + " found");
                            }
                            catch (BGAPI2::Exceptions::AccessDeniedException& /*ex*/) {
                                AddLoggingMessage("    Device " + std::string(camera_pointer->GetModel()) +
                                    " skipped. No access. Maybe the device is used by an other application.");
                            }
                            catch (BGAPI2::Exceptions::LowLevelException& /*ex*/) {
                                AddLoggingMessage("    Device " + std::string(camera_pointer->GetModel()) +
                                    " skipped. No access. Maybe the device is used by an other application.");
                            }
                        }
                    } else {
                        ifc_iter->Close();
                    }
                }
            } else {
                AddLoggingMessage("  Skipped GenTL producer " + std::string(system_pointer->GetPathName()));
            }
        }
        if (!device_found) {
            AddLoggingMessage("No device found");
        }
    }
    catch (BGAPI2::Exceptions::IException& ex) {
        // something goes wrong, thread finished unexpected
        AddLoggingMessage("Init thread finished unexpected!");
        std::stringstream strstream;
        strstream << "Error in function: " << ex.GetFunctionName() << std::endl <<
            "Error description: " << ex.GetErrorDescription() << "Type: " << ex.GetType();
        AddLoggingMessage(strstream.str());
        BGAPI2::SystemList::ReleaseInstance();
        SetInitError();
    }
}

// this function has some special debug code when working with GigE cameras
// the heartbeat mode will be enabled again
// at the end of the function only ReleaseInstance is called
// all the other functions to deinitialise the BGAPI (BGAPI2::System::Close, BGAPI2::Interface::Close)
// will be called automatically by ReleaseInstance
// the called function BGAPI2::Device::Close is also optional
void CameraManager::DeinitializeBGAPI() {
    for (std::vector<Camera*>::iterator camera_iter = cameras_.begin();
        camera_iter != cameras_.end();
        camera_iter++) {
        delete reinterpret_cast<CameraTiming*>(*camera_iter);
    }
    BGAPI2::SystemList::ReleaseInstance();
}

// this thread routine just calls one singe function without loops
// after the function InitializeBGAPI has been processed the initialisation thread ends
void InitThreadRoutine(CameraManager* camera_manager) {
    camera_manager->InitializeBGAPI();
}

// the main loop of the capture thread, leaving this loop means finishing the thread
// wait with the join() function in the main thread (StopCameras) until this loop was left
// the thread also finished, if the capture command fails
// the thread waits in idle state (sleep 10 msec), if the capture command is not active
void CaptureThreadRoutine(CameraManager* camera_manager, Camera * camera) {
    while (!(*camera_manager->FinishWorkerThreads())) {
        // check if the user a activated the capture command
        if (camera->IsCommandCaptureActivatedAndReset()) {
            // function CaptureBGAPIImages blocks until the
            // specified number of images (camera->number_of_images_) was captured
            if (!camera->CaptureBGAPIImages(camera_manager->FinishWorkerThreads(), camera->number_of_images_)) {
                break;
            }
        } else {
            // just wait until capture command become active
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

// the main loop of the feature thread, leaving this loop means finishing the thread
// wait with the join() function in the main thread (StopCameras) until this loop was left
// the thread also finished, if the feature command fails
// the thread waits in idle state (sleep 10 msec), if the feature command is not active
void FeatureThreadRoutine(CameraManager* camera_manager, Camera * camera) {
    // the main loop of the thread, leaving this loop means finishing the thread
    // wait with the join function in the main thread until this loop was left
    while (!(*camera_manager->FinishWorkerThreads())) {
        // check if the user activated the exposure command
        if (camera->IsCommandExposureActivatedAndReset()) {
            if (!camera->ExposureFeatureBGAPI()) {
                break;
            }
        } else {
            // just wait until exposure command become active
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}
