/* Copyright 2019-2020 Baumer Optronic */
#include <iostream>
#include <fstream>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <string>
#include <cstring>
#if defined(_WIN32)
#   include <Windows.h>
#endif
#include "Arguments.h"

void argumentBoolFlag(const Argument& argument, const ArgumentMode mode, const char* /*pParam*/) {
    if (mode == eArgumentInit) {
        *(reinterpret_cast<bool*>(argument.pData)) = (argument.value != 0) ? true : false;
    } else {
        *(reinterpret_cast<bool*>(argument.pData)) = (mode == eArgumentAdd) ? true : false;
    }
}

void argumentUint(const Argument& argument, const ArgumentMode mode, const char* const pParam) {
    unsigned int value = argument.value;
    if (mode != eArgumentInit) {
#if defined(_WIN32)
        if ((pParam != NULL) && (sscanf_s(pParam, "%u", &value) == 1)) {
#else
        if ((pParam != NULL) && (sscanf(pParam, "%u", &value) == 1)) {
#endif
            *((unsigned int*)(argument.pData)) = value;
        }
    }
    *((unsigned int*)(argument.pData)) = value;
}

void argumentString(const Argument& argument, const ArgumentMode mode, const char* const pParam) {
    if (mode == eArgumentInit) {
        // *((std::string*)(argument.pData)) = "";
    } else {
        if (pParam != NULL) {
            *((std::string*)(argument.pData)) = pParam;
        }
    }
}

static char ToLower(int c) {
    return static_cast<char>(::tolower(c));
}

static void LowerString(std::string* const value) {
    if (value != NULL) {
        std::transform(value->begin(), value->end(), value->begin(), ToLower);
    }
}

bool isBoolParam(const char* const pParam, bool* bValue, const char* const pParamOn, const char* const pParamOff) {
    bool bResult = false;
    std::string sParam = pParam;
    LowerString(&sParam);

    std::string sOn = (pParamOn != NULL) ? pParamOn : "on";
    LowerString(&sOn);

    std::string sOff = (pParamOff != NULL) ? pParamOff : "off";
    LowerString(&sOff);

    if (sParam == sOn) {
        *bValue = true;
        bResult = true;
    } else if (sParam == sOff) {
        *bValue = false;
        bResult = true;
    }
    return bResult;
}

// Process single argument
// syntax: -<name>[:<name>...]:<value>*/
static bool processArgument(const char* pString,
    const Argument* pArgumentList,
    const unsigned int argumentCount,
    unsigned int* const pIndex = NULL,
    bool bSkip = false) {
    unsigned int i = (pIndex != NULL) ? (*pIndex + 1) : 0;
    size_t argumentLength = 0;
    const char* pParam = NULL;
    bool bShort = false;

    if (pString == NULL) {
        bSkip = true;
    } else if (pIndex == NULL) {
        // first level - no group
        if (pString[0] != '-') {
            bSkip = true;
        } else {
            pString++;
            if (pString[0] != '-') {
                bShort = true;
            } else {
                pString++;
            }
        }
    }

    if (bSkip == false) {
        pParam = strchr(pString, ':');
        if (pParam == NULL) {
            bSkip = true;
        } else {
            argumentLength = pParam - pString;
            if (argumentLength == 0) {
                bSkip = true;
            } else {
                pParam++;
            }
        }
    }

    if ((pIndex == NULL) && (bSkip != false)) {
        return false;
    }

    while (i < argumentCount) {
        const Argument& entry = pArgumentList[i];
        const char* const pName = bShort ? entry.pShortName : entry.pName;
        if (entry.pProcessArgument != NULL) {
            // Argument value
            if ((bSkip == false) && (pName != NULL)) {
                if (strncmp(pString, pName, argumentLength) == 0) {
                    bool bAdd = false;
                    if ((entry.bFlag != false) && (isBoolParam(pParam, &bAdd) == false)) {
                    } else {
                        entry.pProcessArgument(entry, bAdd ? eArgumentAdd : eArgumentRemove, pParam);
                        return false;
                    }
                }
            }
        } else if ((entry.pName != NULL) || (entry.pShortName != NULL)) {
            // Group start
            const bool bSkipGroup = ((pName == NULL) || (bSkip != false) ||
                (strncmp(pString, pName, argumentLength) != 0));
            if (processArgument(pParam, pArgumentList, argumentCount, &i, bSkipGroup) == false) {
                return false;
            }
        } else {
            // Group end
            if (pIndex != NULL) {
                *pIndex = i;
                return true;
            }
        }

        i++;
    }
    return false;
}

// Compute help text length
static size_t getHelpFieldLength(const Argument* const pArgumentList,
    const unsigned int argumentCount,
    unsigned int * const pIndex = NULL) {
    size_t fieldLength = 0;
    unsigned int i = (pIndex != NULL) ? (*pIndex + 1) : 0;
    while (i < argumentCount) {
        size_t length = 0;
        const size_t lengthShort = ((pIndex == NULL) && (pArgumentList[i].pShortName != NULL)) ?
            strlen(pArgumentList[i].pName) : 0;
        const size_t lengthName = (pArgumentList[i].pName != NULL) ? strlen(pArgumentList[i].pName) : 0;
        if (lengthShort > 0) {
            length = 1 + lengthShort;
        }
        if ((lengthName > 0) && (length < (2 + lengthName))) {
            length = 2 + lengthName;
        }

        if (pArgumentList[i].pProcessArgument != NULL) {
            if ((pArgumentList[i].pDescription1 != NULL) && (pArgumentList[i].pDescription2 != NULL)) {
                length += 1 + strlen(pArgumentList[i].pDescription1);
                if (fieldLength < length) {
                    fieldLength = length;
                }
            }
        } else if (pArgumentList[i].pName != NULL) {
            // Group start
            length += 1 + getHelpFieldLength(pArgumentList, argumentCount, &i);
        } else {
            // Group end
            if (pIndex != NULL) {
                *pIndex = i;
            }
            break;
        }

        if (fieldLength < length) {
            fieldLength = length;
        }
        i++;
    }

    return fieldLength;
}

// Display arguments help text
static void displayHelp(
    const Argument* const pArgumentList,
    const unsigned int argumentCount,
    const std::string sBaseShort,
    const std::string sBaseFull,
    const size_t fieldLength,
    unsigned int * const pIndex = NULL) {
    std::string sArgumentShort;
    std::string sArgumentFull;
    unsigned int i = (pIndex != NULL) ? (*pIndex + 1) : 0;
    while (i < argumentCount) {
        if (pArgumentList[i].pProcessArgument != NULL) {
            if ((pArgumentList[i].pDescription1 != NULL) && (pArgumentList[i].pDescription2 != NULL)) {
                if (pIndex == NULL) {
                    if (pArgumentList[i].pShortName != NULL) {
                        sArgumentShort = "-";
                        sArgumentShort += pArgumentList[i].pShortName;
                        sArgumentShort += ":";
                    }
                    if (pArgumentList[i].pName != NULL) {
                        sArgumentFull = "--";
                        sArgumentFull += pArgumentList[i].pName;
                        sArgumentFull += ":";
                    }
                } else {
                    if (sBaseShort.length() > 0) {
                        sArgumentShort = sBaseShort + pArgumentList[i].pName;
                    }
                    if (sBaseFull.length() > 0) {
                        sArgumentFull = sBaseFull + pArgumentList[i].pName;
                    }
                }

                if ((sArgumentShort.length() > 0) && (sArgumentFull.length() > 0)) {
                    std::cout << sArgumentShort << pArgumentList[i].pDescription1 << std::endl;

                    sArgumentFull += pArgumentList[i].pDescription1;
                    std::cout << std::setw(fieldLength) << std::left << sArgumentFull << " - " <<
                        pArgumentList[i].pDescription2 << std::endl;
                } else if (sArgumentShort.length() > 0) {
                    sArgumentShort += pArgumentList[i].pDescription1;
                    std::cout << std::setw(fieldLength) << std::left << sArgumentShort << " - " <<
                        pArgumentList[i].pDescription2 << std::endl;
                } else if (sArgumentFull.length() > 0) {
                    sArgumentFull += pArgumentList[i].pDescription1;
                    std::cout << std::setw(fieldLength) << std::left << sArgumentFull << " - " <<
                        pArgumentList[i].pDescription2 << std::endl;
                }
            }
        } else if (pArgumentList[i].pName != NULL) {
            // Group start
            sArgumentShort = "";
            sArgumentFull = "";
            if (pIndex == NULL) {
                if (pArgumentList[i].pShortName != NULL) {
                    sArgumentShort = "-";
                    sArgumentShort += pArgumentList[i].pShortName;
                    sArgumentShort += ":";
                }
                if (pArgumentList[i].pName != NULL) {
                    sArgumentFull = "--";
                    sArgumentFull += pArgumentList[i].pName;
                    sArgumentFull += ":";
                }
            } else {
                if (sBaseShort.length() > 0) {
                    sArgumentShort = sBaseShort + ":" + pArgumentList[i].pName;
                }
                if (sBaseFull.length() > 0) {
                    sArgumentFull = sBaseFull + ":" + pArgumentList[i].pName;
                }
            }
            displayHelp(pArgumentList, argumentCount, sArgumentShort, sArgumentFull, fieldLength, &i);
        } else {
            // Group end
            if (pIndex != NULL) {
                *pIndex = i;
            }
            break;
        }
        i++;
    }
}

void parseArguments(const Argument* const pArgumentList, const unsigned int argumentCount, int argc, char* argv[]) {
    // Retrieve arguments maximal help text length
    const size_t fieldLength = getHelpFieldLength(pArgumentList, argumentCount);

    // Display arguments help text
    std::cout << "command line parameters:" << std::endl;
    displayHelp(pArgumentList, argumentCount, "", "", fieldLength);

    // Initialise arguments to default value
    for (unsigned int i = 0; i < argumentCount; i++) {
        if (pArgumentList[i].pProcessArgument != NULL) {
            pArgumentList[i].pProcessArgument(pArgumentList[i], eArgumentInit, NULL);
        }
    }
    std::cout << std::endl;
    std::cout << "default settings can be done over command line parameters or over configuration file 'Config.txt'"
        << std::endl << std::endl;

    // Parse configuration file if any
    std::ifstream configFile;
    configFile.open("Config.txt");
    if (configFile.is_open()) {
        std::string line;
        while (std::getline(configFile, line)) {
            if (line.length() > 0) {
                // Process single argument
                processArgument(line.c_str(), pArgumentList, argumentCount);
            }
        }
        configFile.close();
    }

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        // Process single argument
        processArgument(argv[i], pArgumentList, argumentCount);
    }
}
