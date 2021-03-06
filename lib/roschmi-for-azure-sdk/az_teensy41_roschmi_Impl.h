// Copyright of template
// Copyright (c) Microsoft Corporation. All rights reserved.
// SPDX-License-Identifier: MIT

// Copyright (c) RoSchmi. All rights reserved.
// SPDX-License-Identifier: MIT

// The code of this file provides Teensy 4.1 specific solutions for the 'generic' function calls from
// the azure-sdk-for-c

// These functions are:
//    az_http_client_send_request(...);
//    az_platform_clock_msec(...);
//    az_platform_sleep_msec(...)


#include "az_teensy41_roschmi.h"
#include "EthernetHttpClient_SSL.h"
#include "Ethernet_HTTPClient/Ethernet_HttpClient.h"

EthernetHttpClient * httpClient = NULL;
EthernetClient * reqClient = NULL;
EthernetSSLClient * reqSSLClient = NULL;


const char * PROGMEM mess1 = "-1 Connection refused\r\n\0";
const char * PROGMEM mess2 = "-2 Send Header failed\r\n\0";
const char * PROGMEM mess3 = "-3 Send Payload failed\r\n\0";
const char * PROGMEM mess4 = "-4 Not connected\r\n\0";
const char * PROGMEM mess5 = "-5 Connection lost\r\n\0";
const char * PROGMEM mess6 = "-6 No Stream\r\n\0";
const char * PROGMEM mess7 = "-7 No Http Server\r\n\0";
const char * PROGMEM mess8 = "-8 Too less Ram\r\n\0";
const char * PROGMEM mess9 = "-9 Error Encoding\r\n\0";
const char * PROGMEM mess10 = "-10 Stream Write\r\n\0";
const char * PROGMEM mess11 = "-11 Read Timeout\r\n\0";
const char * PROGMEM mess12 = "-12 unspecified\r\n\0";

void getNextHeader(char * name)
{
    String theName = httpClient->readHeaderName();
    strcpy(name, theName.c_str());
}

/**
 * @brief uses AZ_HTTP_BUILDER to set up request and perform it.
 *
 * @param request an internal http builder with data to build and send http request
 * @param ref_response pre-allocated buffer where http response will be written
 * @return az_result
 */
AZ_NODISCARD az_result
az_http_client_send_request(az_http_request const* request, az_http_response* ref_response)
{
  _az_PRECONDITION_NOT_NULL(request);
  _az_PRECONDITION_NOT_NULL(ref_response);

  // Working with spans
  //https://github.com/Azure/azure-sdk-for-c/tree/master/sdk/docs/core#working-with-spans
  
  az_http_method requMethod = request->_internal.method;
  int32_t max_header_count = request->_internal.max_headers;
  size_t headerCount = az_http_request_headers_count(request);

  

  // Code to copy all headers into one string, actually not needed
  // Was needed for debugging
  /*
  uint8_t headers_buffer[300] {0};
  az_span headers_span = AZ_SPAN_FROM_BUFFER(headers_buffer);
  az_result myResult = dev_az_http_client_build_headers(request, headers_span);
  char* theHeader_str = (char*) az_span_ptr(headers_span);
  volatile size_t headerSize = strlen(theHeader_str);
  */
    
  //az_span_to_str(char* destination, int32_t destination_max_size, az_span source);
    
  az_span urlWorkCopy = request->_internal.url;

  int32_t colonIndex = az_span_find(urlWorkCopy, AZ_SPAN_LITERAL_FROM_STR(":"));
	
  char protocol[6] {0};
  urlWorkCopy = request->_internal.url;
  
  int32_t slashIndex = - 1;
  if (colonIndex != -1)
  {
    az_span_to_str(protocol, 6, az_span_slice(urlWorkCopy, 0, colonIndex));
    if ((strcmp(protocol, (const char *)"https") == 0) || (strcmp(protocol, (const char *)"http") == 0))
    {
      /* protocolIsHttpOrHttps = true; */
    }

    slashIndex = az_span_find(az_span_slice_to_end(urlWorkCopy, colonIndex + 3), AZ_SPAN_LITERAL_FROM_STR("/"));
        
    slashIndex = (slashIndex != -1) ? slashIndex + colonIndex + 3 : -1;       
  }
    
  char workBuffer[100] {0};
    
  if (slashIndex == -1)
  {
    az_span_to_str(workBuffer, sizeof(workBuffer), az_span_slice_to_end(urlWorkCopy, colonIndex + 3));
  }
  else
  {
    az_span_to_str(workBuffer, sizeof(workBuffer), az_span_slice_to_end(az_span_slice(urlWorkCopy, 0, slashIndex), colonIndex + 3));
  }
  String host = (const char *)workBuffer;

  if (slashIndex != -1)
  {
    memset(workBuffer, 0, sizeof(workBuffer));
    az_span_to_str(workBuffer, sizeof(workBuffer), az_span_slice_to_end(urlWorkCopy, slashIndex));
  }
  String resource = slashIndex != -1 ? (const char *)workBuffer : "";

  uint16_t port = (strcmp(protocol, (const char *)"http") == 0) ? 80 : 443;

  httpClient->connectionKeepAlive();
  
  httpClient->noDefaultRequestHeaders();
  
  /*
  Serial.println("Going to send http Request");
  Serial.println((char *)host.c_str());
  Serial.println(port);
  */
  
  // Try 3 times to connect
  for (int i = 0; i < 3; i++)
  {
     if (httpClient->connect((char *)host.c_str(), port))
     {      
       break; 
     }
     else
     {       
        
        #if WORK_WITH_WATCHDOG == 1
          //wdt.feed();
          delay(500);
          //wdt.feed();
        #else
          delay(500);
        #endif
     }
  }
  
  if (httpClient->connected())
  {
    httpClient->beginRequest();
    httpClient->post(resource);
    
    char name_buffer[MAX_HEADERNAME_LENGTH +2] {0};
    char value_buffer[MAX_HEADERVALUE_LENGTH +2] {0};
    az_span head_name = AZ_SPAN_FROM_BUFFER(name_buffer);
    az_span head_value = AZ_SPAN_FROM_BUFFER(value_buffer);
    
    String nameString = "";
    String valueString = "";

    for (int32_t offset = (headerCount - 1); offset >= 0; offset--)
    {
      _az_RETURN_IF_FAILED(az_http_request_get_header(request, offset, &head_name, &head_value));
      
      az_span_to_str((char *)name_buffer, MAX_HEADERNAME_LENGTH -1, head_name);
      az_span_to_str((char *)value_buffer, MAX_HEADERVALUE_LENGTH -1, head_value);
      nameString = (char *)name_buffer;
      valueString = (char *)value_buffer;
    
      httpClient->sendHeader(nameString, valueString);   
    }
    
    httpClient->endRequest();
    httpClient->beginBody();
  
  
    // Printout the body in chunks to avoid allocation of a large buffer)
    
    int32_t maxSliceLength = 50;
    int32_t currIndex = 0;
    int32_t charsToPrint = 0;
    if (request->_internal.body._internal.size > 0)
    {
      int32_t bodySize = request->_internal.body._internal.size;
      charsToPrint = bodySize - currIndex > maxSliceLength ? maxSliceLength : bodySize - currIndex;
      while (charsToPrint > 0)
      {        
        az_span slice = az_span_slice(request->_internal.body, currIndex, currIndex + charsToPrint);    
        currIndex = currIndex + charsToPrint;   
        char buf[maxSliceLength + 1] {0};
        memcpy(buf, slice._internal.ptr, charsToPrint);
        charsToPrint = bodySize - currIndex > maxSliceLength ? maxSliceLength : bodySize - currIndex;
        httpClient->print((char *)buf);      
      } 
    }
    
    if (az_span_is_content_equal(requMethod, AZ_SPAN_LITERAL_FROM_STR("POST")))
    { 
      const int targetHeadersCount = 5;     
      const char headerKeys[targetHeadersCount][20] = {"ETag", "Date", "x-ms-request-id", "x-ms-version", "Content-Type"};

      int httpCode = -1;

      #if WORK_WITH_WATCHDOG == 1
        //wdt.feed();         
      #endif
      
      httpCode = httpClient->responseStatusCode();

      #if WORK_WITH_WATCHDOG == 1
        //wdt.feed();         
      #endif
       
        char httpStatusLine[40] {0};
        if (httpCode > 0)  // Request was successful
        {
          
          sprintf((char *)httpStatusLine, "%s%i%s", "HTTP/1.1 ", httpCode, " ***\r\n");
         __unused az_result appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)httpStatusLine));
          
          #if SERIAL_PRINT == 1
          Serial.println(httpStatusLine);
          #endif

          uint32_t start = millis();

          while(httpClient->headerAvailable() && (millis() -  start) < 200)
          {
            char headerName[35] {0};

            getNextHeader((char *)headerName);   
                
            for (int i = 0; i < targetHeadersCount; i++)
            {                   
                if(strcmp((const char *)headerName, (const char *)headerKeys[i]) == 0)
                {               
                    appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)headerName));          
                    appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)": "));
                    appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)httpClient->readHeaderValue().c_str()));
                    appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)"\r\n"));                   
                    break;
                }
            }                        
          }
          // Add an empty line after the headers
          appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)"\r\n"));
        
          int bodyLength = 0;
          uint32_t readBufferLenght = 51;
          uint8_t readBuffer[readBufferLenght] {0};
          uint32_t timeoutStart = millis();
          uint32_t timeOutLength = 5000;     // Initial timeoutlength is 5000 ms
                    
          // While we haven't timed out & haven't reached the end of the body
          while ( (httpClient->connected() || httpClient->available()) &&
                ((millis() - timeoutStart) < timeOutLength) )
          {
            if (httpClient->available())
            {
              memset(readBuffer, 0, readBufferLenght);  // Clear buffer
              uint32_t numRead = httpClient->read(readBuffer, readBufferLenght - 1);
              appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)readBuffer));
              //Serial.println((char *)readBuffer);
              bodyLength += numRead;

              // We read something, reset the timeout counter
              timeoutStart = millis();
              timeOutLength = 500;   // The next byte must arrive within 500 ms
            }
            else
            {
              // We haven't got any data, so let's pause to allow some to
              // arrive
              delay(100);
            }
          }        
        }
        else  // httpCode <= 0
        {
          Serial.println("Response failed");

          int httpCodeCopy = httpCode;
          char messageBuffer[30] {0};
          // Hack: Convert negative return codes from post request into http codes 401 - 412, so that they can be handeled
          // (returned) as az_http_status_code
                 
          switch (httpCodeCopy)
          {
            case -1: {
            httpCode = 401;
              strcpy(messageBuffer, mess1);
              break;
            }
            case -2: {
              httpCode = 402;
              strcpy(messageBuffer, mess2);
              break;
            }
            case -3: {
              httpCode = 403;
              strcpy(messageBuffer, mess3);
              break;
            }
            case -4: {
              httpCode = 404;
              strcpy(messageBuffer, mess4);
              break;
            }        
            default: {
              httpCode = 404;
              strcpy(messageBuffer, mess12);
            }
          }     
          
          // Request failed because of internal http-client error
          sprintf((char *)httpStatusLine, "%s%i%s%i%s", "HTTP/1.1 ", httpCode, " Http-Client error ", httpCode, " \r\n");
          __unused az_result appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)httpStatusLine));
          appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)"\r\n"));
          appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)messageBuffer));
        }     
    }
    else
    {
      if (az_span_is_content_equal(requMethod, AZ_SPAN_LITERAL_FROM_STR("GET")))
      {
        // Not used
      }
      else
      {
        // Not used
      } 
    }
    //RoSchmi: Stop httpClient to free resources
    httpClient->stop();
  }
  else
  {
    Serial.println(F("Connection failed"));

    char httpStatusLine[40] {0};
    char messageBuffer[30] {0};
    strcpy(messageBuffer, mess4);
    sprintf((char *)httpStatusLine, "%s%i%s%i%s", "HTTP/1.1 ", 404, " Http-Client error ", 404, " \r\n");
    __unused az_result appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)httpStatusLine));
    appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)"\r\n"));
    appendResult = az_http_response_append(ref_response, az_span_create_from_str((char *)messageBuffer));
  }
      
  return AZ_OK;
}

AZ_NODISCARD az_result az_platform_clock_msec(int64_t* out_clock_msec)
{  
  int64_t clPerMico = clockCyclesPerMicrosecond() * 1000;
  out_clock_msec = &clPerMico;
  return AZ_OK;
} 

// For Arduino (Wio Terminal):
AZ_NODISCARD az_result az_platform_sleep_msec(int32_t milliseconds) 
{ 
  delay(milliseconds);
  return AZ_OK;
}

/**
 * @brief initializes the code which performs the request with the initilized httpClient 
 * reqClient: actually not used
 * reqTrustAnchor: actually not used (is already part of httpClient)
 * reqNumTAs: actually not used
 * 
 * @param pReqClient the Ethernet Client
 * @param pEthernetHttpClient the Ethernet http Client (primed with Certificate)
 * @param pTAs Trust Anchors
 * @param pNumTAs Number of Trustanchors
 */

void initializeRequest(EthernetClient * pReqClient, EthernetHttpClient * pEthernetHttpClient, br_x509_trust_anchor pTAs, size_t pNumTAs)
{
  reqClient = pReqClient;
  httpClient = pEthernetHttpClient;
}

/**
 * @brief loop all the headers from a HTTP request and combine all headers into one az_span
 *
 * @param request an http builder request reference
 * @param ref_headers list of headers
 * @return az_result
 */

/*
// For debugging
static AZ_NODISCARD az_result
dev_az_http_client_build_headers(az_http_request const* request, az_span ref_headers)
{
  _az_PRECONDITION_NOT_NULL(request);
  
  az_span header_name = { 0 };
  az_span header_value = { 0 };
  uint8_t header_buffer[request->_internal.url_length - 20 + 60] {0};
  az_span header_span = AZ_SPAN_FROM_BUFFER(header_buffer);
  az_span separator = AZ_SPAN_LITERAL_FROM_STR(": ");
  

  for (int32_t offset = 0; offset < az_http_request_headers_count(request); ++offset)
  {
    _az_RETURN_IF_FAILED(az_http_request_get_header(request, offset, &header_name, &header_value));   
    _az_RETURN_IF_FAILED(dev_az_span_append_header_to_buffer(header_span, header_name, header_value, separator));
    char* header_str = (char*) az_span_ptr(header_span); // str points to a 0-terminated string
    ref_headers = az_span_copy(ref_headers, az_span_create_from_str(header_str));
    ref_headers = az_span_copy(ref_headers, AZ_SPAN_LITERAL_FROM_STR("\r\n"));
  }
  az_span_copy_u8(ref_headers, 0);
  return AZ_OK;
}
*/

/**
 * @brief writes a header key and value to a buffer as a 0-terminated string and using a separator
 * span in between. Returns error as soon as any of the write operations fails
 *
 * @param writable_buffer pre allocated buffer that will be used to hold header key and value
 * @param header_name header name
 * @param header_value header value
 * @param separator symbol to be used between key and value
 * @return az_result
 */


static AZ_NODISCARD az_result dev_az_span_append_header_to_buffer(
    az_span writable_buffer,
    az_span header_name,
    az_span header_value,
    az_span separator)
{
  int32_t required_length
      = az_span_size(header_name) + az_span_size(separator) + az_span_size(header_value) + 1;

  _az_RETURN_IF_NOT_ENOUGH_SIZE(writable_buffer, required_length);

  writable_buffer = az_span_copy(writable_buffer, header_name);
  writable_buffer = az_span_copy(writable_buffer, separator);
  writable_buffer = az_span_copy(writable_buffer, header_value);
  az_span_copy_u8(writable_buffer, 0);
  return AZ_OK;
}
