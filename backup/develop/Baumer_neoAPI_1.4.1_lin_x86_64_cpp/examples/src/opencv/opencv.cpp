/** \example opencv.cpp
    A simple Program for grabbing video from Baumer camera and converting it to opencv images.
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/

#include <stdio.h>
#include <iostream>
#include <string>
#include <map>
#include <opencv2/highgui.hpp>
#include "neoapi/neoapi.hpp"

struct PixelFormatStruct {
    PixelFormatStruct() {}
    PixelFormatStruct(uint32_t _code, int _type, const char* _target, double _scale) {
        code = _code; type = _type; target = _target; scale = _scale;
    }
    uint32_t code{ 0 };
    int      type{ 0 };
    const char*    target{ "" };
    double   scale{ 1.0 };
};

#define AE(PF, PC, TY, CPF, scale) \
    pixelFormatLUT.insert(std::make_pair(PF, PixelFormatStruct(PC, TY, CPF, scale)));

PixelFormatStruct get_mapping(const char * pixelformat) {
    std::map<std::string, PixelFormatStruct> pixelFormatLUT;

    AE("Mono8",              0x01080001, CV_8UC1,  "",           1.0);
    AE("Mono8",              0x01080001, CV_8UC1,  "",           1.0);
    AE("Mono8s",             0x01080002, CV_8SC1,  "",           1.0);
    AE("Mono10",             0x01100003, CV_16UC1, "",           64.0);
    AE("Mono10Packed",       0x010C0004, CV_16UC1, "Mono10",     64.0);
    AE("Mono12",             0x01100005, CV_16UC1, "",           16.0);
    AE("Mono12Packed",       0x010C0006, CV_16UC1, "Mono12",     16.0);
    AE("Mono16",             0x01100007, CV_16UC1, "",           1.0);
    AE("BayerGR8",           0x01080008, CV_8UC1,  "",           1.0);  // "BGR8"
    AE("BayerRG8",           0x01080009, CV_8UC1,  "",           1.0);  // "BGR8"
    AE("BayerGB8",           0x0108000A, CV_8UC1,  "",           1.0);  // "BGR8",
    AE("BayerBG8",           0x0108000B, CV_8UC1,  "",           1.0);  // "BGR8",
    AE("BayerGR10",          0x0110000C, CV_16UC1, "",           64.0);  // "BGR10"
    AE("BayerRG10",          0x0110000D, CV_16UC1, "",           64.0);  // "BGR10"
    AE("BayerGB10",          0x0110000E, CV_16UC1, "",           64.0);  // "BGR10"
    AE("BayerBG10",          0x0110000F, CV_16UC1, "",           64.0);  // "BGR10"
    AE("BayerGR12",          0x01100010, CV_16UC1, "",           16.0);  // "BGR12"
    AE("BayerRG12",          0x01100011, CV_16UC1, "",           16.0);  // "BGR12"
    AE("BayerGB12",          0x01100012, CV_16UC1, "",           16.0);  // "BGR12"
    AE("BayerBG12",          0x01100013, CV_16UC1, "",           16.0);  // "BGR12"
    AE("RGB8",               0x02180014, CV_8UC3,  "",           1.0);  // "BGR8"
    AE("BGR8",               0x02180015, CV_8UC3,  "",           1.0);
    AE("RGBa8",              0x02200016, CV_8UC4,  "",           1.0);  // "BGR8"
    AE("BGRa8",              0x02200017, CV_8UC4,  "",           1.0);  // "BGR8"
    AE("RGB10",              0x02300018, CV_16UC3, "",           64.0);  // "BGR10"
    AE("BGR10",              0x02300019, CV_16UC3, "",           64.0);
    AE("RGB12",              0x0230001A, CV_16UC3, "",           16.0);  // "BGR12"
    AE("BGR12",              0x0230001B, CV_16UC3, "",           16.0);
    AE("YUV411_8_UYYVYY",    0x020C001E, CV_8UC3,  "BGR8",       1.0);
    AE("YUV422_8_UYVY",      0x0210001F, CV_8UC3,  "BGR8",       1.0);
    AE("YUV8_UYV",           0x02180020, CV_8UC3,  "BGR8",       1.0);
    AE("RGB8_Planar",        0x02180021, CV_8UC3,  "",           1.0);  // "BGR8"
    AE("RGB10_Planar",       0x02300022, CV_16UC3, "",           64.0);  // "BGR10"
    AE("RGB12_Planar",       0x02300023, CV_16UC3, "",           16.0);  // "BGR10"
    AE("RGB16_Planar",       0x02300024, CV_16UC3, "",           1.0);  // "BGR10"
    AE("Mono14",             0x01100025, CV_16UC1, "",           4.0);
    AE("BayerGR10Packed",    0x010C0026, CV_16UC1, "BayerGR10",  64.0);
    AE("BayerRG10Packed",    0x010C0027, CV_16UC1, "BayerRG10",  64.0);
    AE("BayerGB10Packed",    0x010C0028, CV_16UC1, "BayerGB10",  64.0);
    AE("BayerBG10Packed",    0x010C0029, CV_16UC1, "BayerBG10",  64.0);
    AE("BayerGR12Packed",    0x010C002A, CV_16UC1, "BayerGR12",  16.0);
    AE("BayerRG12Packed",    0x010C002B, CV_16UC1, "BayerRG12",  16.0);
    AE("BayerGB12Packed",    0x010C002C, CV_16UC1, "BayerGB12",  16.0);
    AE("BayerBG12Packed",    0x010C002D, CV_16UC1, "BayerBG12",  16.0);
    AE("BayerGR16",          0x0110002E, CV_16UC1, "",           1.0);
    AE("BayerRG16",          0x0110002F, CV_16UC1, "",           1.0);
    AE("BayerGB16",          0x01100030, CV_16UC1, "",           1.0);
    AE("BayerBG16",          0x01100031, CV_16UC1, "",           1.0);
    AE("RGB16",              0x02300033, CV_16UC3, "",           1.0);  // "BGR16"
    AE("Mono10p",            0x010A0046, CV_16UC1, "Mono10",     64.0);
    AE("Mono12p",            0x010C0047, CV_16UC1, "Mono12",     16.0);
    AE("BGR12p",             0x02240049, CV_16UC3, "BGR16",      16.0);
    AE("BGR14",              0x0230004A, CV_16UC3, "BGR16",      4.0);
    AE("BGR16",              0x0230004B, CV_16UC3, "BGR16",      1.0);
    AE("BayerBG10p",         0x010A0052, CV_16UC1, "BayerBG10",  64.0);
    AE("BayerBG12p",         0x010C0053, CV_16UC1, "BayerBG12",  16.0);
    AE("BayerGB10p",         0x010A0054, CV_16UC1, "BayerGB10",  64.0);
    AE("BayerGB12p",         0x010C0055, CV_16UC1, "BayerGB12",  11.0);
    AE("BayerGR10p",         0x010A0056, CV_16UC1, "BayerGR10",  64.0);
    AE("BayerGR12p",         0x010C0057, CV_16UC1, "BayerGR12",  16.0);
    AE("BayerRG10p",         0x010A0058, CV_16UC1, "BayerRG10",  64.0);
    AE("BayerRG12p",         0x010C0059, CV_16UC1, "BayerRG12",  16.0);
    AE("RGB12p",             0x0224005D, CV_16UC3, "BGR12",      16.0);
    AE("RGB14",              0x0230005E, CV_16UC3, "",           4.0);
    AE("R8",                 0x010800C9, CV_8UC1,  "",           1.0);
    AE("G8",                 0x010800CD, CV_8UC1,  "",           1.0);
    AE("B8",                 0x010800D1, CV_8UC1,  "",           1.0);
    AE("R10",                0x011000CA, CV_16UC1, "",           64.0);
    AE("R12",                0x011000CB, CV_16UC1, "",           16.0);
    AE("G10",                0x011000CE, CV_16UC1, "",           64.0);
    AE("G12",                0x011000CF, CV_16UC1, "",           16.0);
    AE("B10",                0x011000D2, CV_16UC1, "",           64.0);
    AE("B12",                0x011000D3, CV_16UC1, "",           16.0);
    AE("R12p",               0x820C0447, CV_16UC1, "R12",        16.0);
    AE("B12p",               0x820C0147, CV_16UC1, "B12",        16.0);
    AE("G12p",               0x820C0247, CV_16UC1, "G12",        16.0);
    AE("BaumerPolarized8",   0x81080100, CV_8UC1,  "Mono8",      1.0);
    AE("BaumerPolarized10",  0x81100101, CV_8UC1,  "Mono8",      1.0);
    AE("BaumerPolarized12",  0x81100102, CV_8UC1,  "Mono8",      1.0);
    AE("BaumerPolarized12p", 0x810C0102, CV_8UC1,  "Mono8",      1.0);
    // aliases
    AE("RGB8Packed",         0x02180014, CV_8UC3,  "",           1.0);  // "RGB8"
    AE("BGR8Packed",         0x02180015, CV_8UC3,  "",           1.0);  // "BGR8"
    AE("RGB10Packed",        0x02300018, CV_16UC3, "",           64.0);  // "RGB10"
    AE("BGR10Packed",        0x02300019, CV_16UC3, "",           64.0);  // "BGR10"
    AE("BGR10",              0x02300019, CV_16UC3, "",           64.0);
    AE("RGB12Packed",        0x0230001A, CV_16UC3, "",           16.0);  // "RGB12"
    AE("BGR12Packed",        0x0230001B, CV_16UC3, "",           16.0);  // "BGR12"
    AE("BGR12",              0x0230001B, CV_16UC3, "",           16.0);
    AE("RGBA8Packed",        0x02200016, CV_8UC4,  "",           1.0);  // "RGBa8"
    AE("BGRA8Packed",        0x02200017, CV_8UC4,  "",           1.0);  // "BGRa8"
    AE("YUV411Packed",       0x020C001E, CV_8UC3,  "BGR8",       1.0);  // "YUV411_8_UYYVYY"
    AE("YUV422Packed",       0x0210001F, CV_8UC3,  "BGR8",       1.0);  // "YUV422_8_UYVY"
    AE("YUV444Packed",       0x02180020, CV_8UC3,  "BGR8",       1.0);  // "YUV8_UYV"
    AE("YUV8",               0x02180020, CV_8UC3,  "BGR8",       1.0);  // "YUV8_UYV"
    AE("Mono8Signed",        0x01080002, CV_8SC1,  "",           1.0);  // "Mono8s"
    AE("RGB16Packed",        0x02300033, CV_16UC3, "",           1.0);  // "RGB16"
    AE("BGR16Packed",        0x0230004B, CV_16UC3, "BGR16",      1.0);  // "BGR16"

    if (pixelFormatLUT.find(pixelformat) != pixelFormatLUT.end()) {
        return pixelFormatLUT[pixelformat];
    } else {
        return PixelFormatStruct(0x01080001, CV_8UC1, "", 1.0);
    }
}

int main() {
    int result = 0;
    try {
        NeoAPI::Cam camera = NeoAPI::Cam();
        camera.Connect();
        camera.f().ExposureTime.Set(10000);

        PixelFormatStruct data = get_mapping(camera.f().PixelFormat.GetString());

        int width = static_cast<int>(camera.f().Width);
        int height = static_cast<int>(camera.f().Height);
        std::string title = "pixelformat cam:";
        title += camera.f().PixelFormat.GetString();
        title += " image:";
        title += data.target;
        title += "; Press [ESC] to exit ..";

        bool save_image = true;
        const cv::String windowName = title.c_str();
        for (int count = 0; count < 90000; ++count) {
            NeoAPI::Image image = camera.GetImage();

            void *buf;
            if (data.target && *data.target) {
                if (image.IsPixelFormatAvailable(data.target)) {
                    image = image.Convert(data.target);
                } else {
                    throw;
                }
            }
            buf = image.GetImageData();

            cv::Mat img(cv::Size(width, height), data.type, buf, cv::Mat::AUTO_STEP);
            if (data.scale != 1.0) {
                img.convertTo(img, data.type, data.scale);
            }
	/*
            cv::namedWindow(windowName);
            cv::imshow(windowName, img);
            if (save_image) {
                save_image = false;
                cv::imwrite("opencv_cpp.bmp", img);
            }
            if (cv::waitKey(1) == 27) {
                break;
            }
        }
        cv::destroyWindow(windowName);
	*/
    }
    catch (NeoAPI::NeoException& exc) {
        std::cout << "error: " << exc.GetDescription() << std::endl;
        result = 1;
    }
    catch (std::exception& exc) {
        std::cout << "oops, error" << "[" << exc.what() << "]" << std::endl;
        result = 1;
    }
    catch (...) {
        std::cout << "oops, error" << std::endl;
        result = 1;
    }

    return result;
}
