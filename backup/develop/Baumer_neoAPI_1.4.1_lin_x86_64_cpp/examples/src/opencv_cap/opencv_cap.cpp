/** \example opencv.cpp
    A simple Program for grabbing video from Baumer camera and converting it to opencv images.
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/

#include <stdio.h>
#include <iostream>
#include <opencv2/highgui.hpp>
#include "neoapi/neoapi.hpp"
#include <nadjieb/mjpeg_streamer.hpp>
int main() {
    int result = 0;
    try {
        NeoAPI::Cam camera = NeoAPI::Cam();
	nadjieb::MJPEGStreamer streamer;
	std::vector<int> params = {cv::IMWRITE_JPEG_QUALITY, 90};
        camera.Connect();
        camera.f().ExposureTime.Set(10000);

        int type = CV_8U;
        bool isColor = true;
        if (camera.f().PixelFormat.GetEnumValueList().IsReadable("BGR8")) {
            camera.f().PixelFormat.SetString("BGR8");
            type = CV_8UC3;
            isColor = true;
        } else if (camera.f().PixelFormat.GetEnumValueList().IsReadable("Mono8")) {
            camera.f().PixelFormat.SetString("Mono8");
            type = CV_8UC1;
            isColor = false;
        } else {
            std::cout << "no supported pixel format";
            return 0;  // Camera does not support pixelformat
        }
        int width = static_cast<int>(camera.f().Width);
        int height = static_cast<int>(camera.f().Height);

	streamer.start(9000);
	while (streamer.isRunning())
	{
            NeoAPI::Image image = camera.GetImage();
            cv::Mat img(cv::Size(width, height), type, image.GetImageData(), cv::Mat::AUTO_STEP);
	std::vector<uchar> buff_bgr;
        cv::imencode(".jpg", img, buff_bgr, params);
        streamer.publish("/bgr", std::string(buff_bgr.begin(), buff_bgr.end()));
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
        streamer.stop();	
    }
    catch (NeoAPI::NeoException& exc) {
        std::cout << "error: " << exc.GetDescription() << std::endl;
        result = 1;
    }
    catch (...) {
        std::cout << "oops, error" << std::endl;
        result = 1;
    }

    return result;
}
