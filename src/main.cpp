// Application AzureDataSender_Teensy_QnEthernet
// for Teensy 4.1
// Copyright RoSchmi 2020, 2021. License Apache 2.0


// Cave !!!!!
// Replace the Stream.h file in  C:/Users/thisUser/.platformio/packages/framework-arduinoteensy/cores/teensy4/Stream.h
// with the custom version found in lib/RoSchmi/Stream 

// This version 
// but the new QNEthernet library:
// https://github.com/ssilverman/QNEthernet   (6. Sep. 2021)
//
// This new Ethernet library for the Teensy 4.1 is now supported by
// https://github.com/khoih-prog/EthernetWebServer_SSL which is used in this application

// Besides this version, a version using the 'NativeEthernet' and 'FNET' libraries, which were by default used with Teensy 4.1
// is available:
// https://github.com/RoSchmi/AzureDataSender_Teensy


// Tips for debugging
// Main: include/config.h #define SERIAL_PRINT 1

// In config.h you can select that simulated sensor values are used. Comment the line
// #define USE_SIMULATED_SENSORVALUES  in config.h if not wanted   

// Special files in folder include/:
// *********************************
// defines.h:
//     Defines settings which adapt Ethernet- and SSL-libraries to Teensy 4.1

// trust_anchors.h:
//     Holds Certificates to be used in SSL-libraries

// config.h
//     Contains settings like sendinterval, timezone, daylightsavingstime settings, transport protocol, 
//      tablenames and so on

// config_secret.h
//      Contains credentials of used Azure Storage Account
//      This file should be excluded from transfer to GitHub through the .gitignore file in folder include/

// config_secret_template.h
//      This file is a template for config_secret.h


  // I started to make this App following the Azure Storage Blob Example as a template, see: 
  // https://github.com/Azure/azure-sdk-for-c/blob/5c7444dfcd5f0b3bcf3aec2f7b62639afc8bd664/sdk/samples/storage/blobs/src/blobs_client_example.c

  // The used Azure-sdk-for-c libraries were Vers. 1.1.0
  // https://github.com/Azure/azure-sdk-for-c/releases/tag/1.1.0
  // In Vers. 1.1.0 were some changes needed in the file az_http_internal.h
  // the changes are included in this repo


#include <Arduino.h>
#include "Watchdog_t4.h"
#include "defines.h"
#include "Entropy.h"
#include "DateTime.h"
#include "SysTime.h"
#include "config.h"
#include "config_secret.h"
// SSL Certificates for server verfication are in this file
#include "trust_anchors.h"

#include "CloudStorageAccount.h"
#include "TableClient.h"
#include "TableEntityProperty.h"
#include "TableEntity.h"
#include "AnalogTableEntity.h"
#include "OnOffTableEntity.h"

#include "NTPClient_Generic.h"
#include "Timezone_Generic.h"
#include "EthernetHttpClient_SSL.h"
#include "EthernetWebServer_SSL.h"
#include "functional-vlpp.h"
#include "DataContainerWio.h"
#include "OnOffDataContainerWio.h"
#include "OnOffSwitcherWio.h"
#include "ImuManagerWio.h"
#include "AnalogSensorMgr.h"

#include "az_teensy41_roschmi.h"

#include "azure/core/az_platform.h"
#include "azure/core/az_http.h"
#include "azure/core/az_http_transport.h"
#include "azure/core/az_result.h"
#include "azure/core/az_config.h"
#include "azure/core/az_context.h"
#include "azure/core/az_span.h"

#include "Rs_TimeNameHelper.h"

// only needed for tests
uint8_t lower_buffer[32];
uint8_t upper_buffer[32] DMAMEM;

const char analogTableName[45] = ANALOG_TABLENAME;

const char OnOffTableName_1[45] = ON_OFF_TABLENAME_01;
const char OnOffTableName_2[45] = ON_OFF_TABLENAME_02;
const char OnOffTableName_3[45] = ON_OFF_TABLENAME_03;
const char OnOffTableName_4[45] = ON_OFF_TABLENAME_04;

// The PartitionKey for the analog table may have a prefix to be distinguished, here: "Y2_" 
const char * analogTablePartPrefix = (char *)ANALOG_TABLE_PART_PREFIX;

// The PartitionKey for the On/Off-tables may have a prefix to be distinguished, here: "Y3_" 
const char * onOffTablePartPrefix = (char *)ON_OFF_TABLE_PART_PREFIX;

// The PartitinKey can be augmented with a string representing year and month (recommended)
const bool augmentPartitionKey = true;

// The TableName can be augmented with the actual year (recommended)
const bool augmentTableNameWithYear = true;

// Define Datacontainer with SendInterval and InvalidateInterval as defined in config.h
int sendIntervalSeconds = (SENDINTERVAL_MINUTES * 60) < 1 ? 1 : (SENDINTERVAL_MINUTES * 60);

DataContainerWio dataContainer(TimeSpan(sendIntervalSeconds), TimeSpan(0, 0, INVALIDATEINTERVAL_MINUTES % 60, 0), (float)MIN_DATAVALUE, (float)MAX_DATAVALUE, (float)MAGIC_NUMBER_INVALID);

AnalogSensorMgr analogSensorMgr(MAGIC_NUMBER_INVALID);

OnOffDataContainerWio onOffDataContainer;

OnOffSwitcherWio onOffSwitcherWio;

ImuManagerWio imuManagerWio;

WDT_T4<WDT1> wdt;


// Do not delete
/*
#define DEBUG 1
#define LOG_USB 1
#define ACTLOGLEVEL = LOG_DEBUG_V3
#define DebugLevel = SSL_INFO
*/

volatile uint64_t loopCounter = 0;
unsigned int insertCounterAnalogTable = 0;
uint32_t tryUploadCounter = 0;
uint32_t timeNtpUpdateCounter = 0;
volatile int32_t sysTimeNtpDelta = 0;


//uint32_t ntpUpdateInterval = 60000;

bool ledState = false;
uint8_t lastResetCause = -1;

bool sendResultState = true;

uint32_t failedUploadCounter = 0;

const int timeZoneOffset = (int)TIMEZONEOFFSET;
const int dstOffset = (int)DSTOFFSET;


// A UDP instance to let us send and receive packets over UDP
EthernetUDP ntpUDP;

// Class to access RTC, must be static
static SysTime sysTime;

Rs_TimeNameHelper timeNameHelper;

// Set transport protocol as defined in config.h
static bool UseHttps_State = TRANSPORT_PROTOCOL == 0 ? false : true;

CloudStorageAccount myCloudStorageAccount(AZURE_CONFIG_ACCOUNT_NAME, AZURE_CONFIG_ACCOUNT_KEY, UseHttps_State);
CloudStorageAccount * myCloudStorageAccountPtr = &myCloudStorageAccount;

// forward declarations


String floToStr(float value);
float ReadAnalogSensor(int pSensorIndex);
void createSampleTime(DateTime dateTimeUTCNow, int timeZoneOffsetUTC, char * sampleTime);
az_http_status_code  createTable(CloudStorageAccount * myCloudStorageAccountPtr, const char * tableName);
az_http_status_code CreateTable( const char * tableName, ContType pContentType, AcceptType pAcceptType, ResponseType pResponseType, bool);
az_http_status_code insertTableEntity(CloudStorageAccount *myCloudStorageAccountPtr, const char * pTableName, TableEntity pTableEntity, char * outInsertETag);
void makePartitionKey(const char * partitionKeyprefix, bool augmentWithYear, DateTime dateTime, az_span outSpan, size_t *outSpanLength);
void makeRowKey(DateTime actDate, az_span outSpan, size_t *outSpanLength);
int getDayNum(const char * day);
int getMonNum(const char * month);
int getWeekOfMonthNum(const char * weekOfMonth);

NTPClient timeClient(ntpUDP);

DateTime dateTimeUTCNow;    // Seconds since 2000-01-01 08:00:00

Timezone myTimezone;

void watchDogCallback() {
  Serial.println(F("FEED THE DOG SOON, OR RESET!"));
}

void setup(){

  // Used for debugging
  /*
  uint8_t stack_buffer[32];
  uint32_t * heap_buffer;
  heap_buffer = (uint32_t *)malloc(32);
  */
  
  Serial.begin(115200);
  //while (!Serial);
  
  // Used for debugging, shows some memory areas
  /*
  delay(500);
  Serial.printf("Lower_Buffer: %x\n", (uint32_t)lower_buffer);
  Serial.printf("upper_buffer: %x\n", (uint32_t)upper_buffer);
  Serial.printf("stack buffer: %x\n", (uint32_t)stack_buffer);
  Serial.printf("Heap Buffer : %x\n", (uint32_t)heap_buffer);
  */

  // This code snippet can be used to get the addresses of the heap
  // and to 
  //uint32_t * ptr_one;
  //uint32_t * last_ptr_one;
    
  /*
   for (volatile int i = 0; 1 < 100000; i++)
   {
     last_ptr_one = ptr_one;
     ptr_one = 0;
     ptr_one = (uint32_t *)malloc(1);
     if (ptr_one == 0)
     {
       ptr_one = last_ptr_one;
       volatile int dummy68424 = 1;
     }
     else
     {
       *ptr_one = (uint32_t)0xAA55AA55;
       char printBuf[25];
       
       if (((uint32_t)ptr_one % 256)== 0)
       {
         sprintf(printBuf, "%09x", (uint32_t)ptr_one);
          lcd_log_line((char*)printBuf);
       }
     } 
   }
   */
   
  // Fills memory from 0x20028F80 - 0x2002FE00 with pattern AA55
  // So you can see at breakpoints how much of heap was used
  /*
  ptr_one = (uint32_t *)0x20028F80;
  while (ptr_one < (uint32_t *)0x2002fe00)
  {
    *ptr_one = (uint32_t)0xAA55AA55;
     ptr_one++;
  }
  */

  // Wait some time (3000 ms)
  uint32_t start = millis();
  while ((millis() - start) < 3000)
  {
    delay(10);
  }

  pinMode(LED_BUILTIN, OUTPUT);
  Serial.print("\nStart Ethernet_NTPClient_Basic on " + String(BOARD_NAME));
  Serial.println(" with " + String(SHIELD_TYPE));

  onOffDataContainer.begin(DateTime(), OnOffTableName_1, OnOffTableName_2, OnOffTableName_3, OnOffTableName_4);

  // Initialize State of 4 On/Off-sensor representations 
  // and of the inverter flags (Application specific)
  for (int i = 0; i < 4; i++)
  {
    onOffDataContainer.PresetOnOffState(i, false, true);
    onOffDataContainer.Set_OutInverter(i, true);
    onOffDataContainer.Set_InputInverter(i, false);
  }

  //Initialize OnOffSwitcher (for tests and simulation)
  onOffSwitcherWio.begin(TimeSpan(15 * 60));   // Toggle every 30 min
  onOffSwitcherWio.SetInactive();
  //onOffSwitcherWio.SetActive();


// buffer to hold messages
  char buf[100];

// Save copy of Reset Status register 
  lastResetCause = SRC_SRSR;

  // Clear all reset status register bits with
  SRC_SRSR = (uint32_t)0x7F;

  // Print the last reset cause
  Serial.println(F("Reset happened after:"));
  if ((lastResetCause & SRC_SRSR_IPP_RESET_B) != 0) {
    Serial.println(F("Power Reset."));
  }

  if ((lastResetCause & SRC_SRSR_LOCKUP_SYSRESETREQ) != 0) {
    Serial.println(F("Software Reset."));
  }

  if ((lastResetCause & SRC_SRSR_WDOG_RST_B) !=0) {
    Serial.println(F("Watchdog Reset."));
  }
  
  Serial.print(F("Last state of 'ResetCause' Register: "));

  sprintf(buf, "0x%08x", lastResetCause);
  Serial.println(buf);

  #if WORK_WITH_WATCHDOG == 1
    WDT_timings_t config;
    config.trigger = 1; /* seconds before reset occurs, 0->128 */
    config.timeout = 30; /* in seconds, 0->128 */
    config.callback = watchDogCallback;
    wdt.begin(config);
  #endif

  // Setting Daylightsavingtime. Enter values for your zone in file include/config.h
  // Program aborts in some cases of invalid values
  
  int dstWeekday = getDayNum(DST_START_WEEKDAY);
  int dstMonth = getMonNum(DST_START_MONTH);
  int dstWeekOfMonth = getWeekOfMonthNum(DST_START_WEEK_OF_MONTH);

  TimeChangeRule dstStart {DST_ON_NAME, (uint8_t)dstWeekOfMonth, (uint8_t)dstWeekday, (uint8_t)dstMonth, DST_START_HOUR, TIMEZONEOFFSET + DSTOFFSET};
  
  bool firstTimeZoneDef_is_Valid = (dstWeekday == -1 || dstMonth == - 1 || dstWeekOfMonth == -1 || DST_START_HOUR > 23 ? true : DST_START_HOUR < 0 ? true : false) ? false : true;
  
  dstWeekday = getDayNum(DST_STOP_WEEKDAY);
  // RoSchmi
  // changed
  //dstMonth = getMonNum(DST_STOP_MONTH) + 1;
  dstMonth = getMonNum(DST_STOP_MONTH);

  dstWeekOfMonth = getWeekOfMonthNum(DST_STOP_WEEK_OF_MONTH);

  TimeChangeRule stdStart {DST_OFF_NAME, (uint8_t)dstWeekOfMonth, (uint8_t)dstWeekday, (uint8_t)dstMonth, (uint8_t)DST_START_HOUR, (int)TIMEZONEOFFSET};

  bool secondTimeZoneDef_is_Valid = (dstWeekday == -1 || dstMonth == - 1 || dstWeekOfMonth == -1 || DST_STOP_HOUR > 23 ? true : DST_STOP_HOUR < 0 ? true : false) ? false : true;
  
  if (firstTimeZoneDef_is_Valid && secondTimeZoneDef_is_Valid)
  {
    myTimezone.setRules(dstStart, stdStart);
  }
  else
  {
    // If timezonesettings are not valid: -> take UTC and wait for ever  
    TimeChangeRule stdStart {DST_OFF_NAME, (uint8_t)dstWeekOfMonth, (uint8_t)dstWeekday, (uint8_t)dstMonth, (uint8_t)DST_START_HOUR, (int)0};
    myTimezone.setRules(stdStart, stdStart);
    while (true)
    {
      Serial.println("Invalid DST Timezonesettings");
      delay(5000);
    }
  }

  #if USE_ETHERNET_WRAPPER
  EthernetInit();
  #endif

  // start the ethernet connection and the server:
  // Use DHCP dynamic IP and random mac
  uint16_t index = millis() % NUMBER_OF_MAC;

  Ethernet.macAddress(mac[index]);

  #if WORK_WITH_WATCHDOG == 1
    wdt.feed();
  #endif
  
  // Set DNS sever to your choice (if wanted)
  /*
  uint8_t theDNS_Server_Ip[4] {8,8,8,8};
  Ethernet.setDnsServerIP(theDNS_Server_Ip);
  */

  #if USE_STATIC_IP
  // Set ip address of your choice
  IPAddress ip = {192, 168, 1, 106};
  Ethernet.begin(mac[index], ip);    // This is still code for NativeEthernet, not yet tested with QnEthernet library
  #else
  Serial.println(F("Starting DHCP"));
  Ethernet.begin();
  Ethernet.waitForLocalIP(10000);
  if (Ethernet.localIP() == INADDR_NONE)
  {
    Serial.println("Failed to configure Ethernet using DHCP");
  }
  #endif

  #if WORK_WITH_WATCHDOG == 1
    wdt.feed();
  #endif
  
  // Just info to know how to connect correctly
  Serial.println(F("========================="));
  Serial.println(F("Currently Used SPI pinout:"));
  Serial.print(F("MOSI:"));
  Serial.println(MOSI);
  Serial.print(F("MISO:"));
  Serial.println(MISO);
  Serial.print(F("SCK:"));
  Serial.println(SCK);
  Serial.print(F("SS:"));
  Serial.println(SS);

  Serial.println(F("========================="));

  Serial.print(F("Using mac index = "));
  Serial.println(index);

  // you're connected now, so print out the data
  Serial.print(F("You're connected to the network, IP = "));
  Serial.println(Ethernet.localIP());

  timeClient.begin();
  timeClient.setUpdateInterval((NTP_UPDATE_INTERVAL_MINUTES < 1 ? 1 : NTP_UPDATE_INTERVAL_MINUTES) * 60 * 1000);
  // 'setRetryInterval' should not be too short, may be that short intervals lead to malfunction 
  timeClient.setRetryInterval(20000);  // Try to read from NTP Server not more often than every 20 seconds
  Serial.println("Using NTP Server " + timeClient.getPoolServerName());
  
  timeClient.update();
  uint32_t counter = 0;
  uint32_t maxCounter = 10;
  
  while(!timeClient.updated() &&  counter++ <= maxCounter)
  {
    Serial.println(F("NTP FAILED: Trying again"));
    delay(1000);
    #if WORK_WITH_WATCHDOG == 1
      wdt.feed();
    #endif
    timeClient.update();
  }

  if (counter >= maxCounter)
  {
    while(true)
    {
      delay(500); //Wait for ever, could not get NTP time, eventually reboot by Watchdog
    }
  }
  
  Serial.println("\r\n********UPDATED********");
    
  Serial.println("UTC : " + timeClient.getFormattedUTCTime());
  Serial.println("UTC : " + timeClient.getFormattedUTCDateTime());
  Serial.println("LOC : " + timeClient.getFormattedTime());
  Serial.println("LOC : " + timeClient.getFormattedDateTime());
  Serial.println("UTC EPOCH : " + String(timeClient.getUTCEpochTime()));
  Serial.println("LOC EPOCH : " + String(timeClient.getEpochTime()));

  unsigned long utcTime = timeClient.getUTCEpochTime();  // Seconds since 1. Jan. 1970

  sysTime.begin(utcTime + SECONDS_FROM_1970_TO_2000);

  dateTimeUTCNow = sysTime.getTime();
  
  Serial.printf("%s %i %i %i %i %i", (char *)"UTC-Time is  :", dateTimeUTCNow.year(), 
                                        dateTimeUTCNow.month() , dateTimeUTCNow.day(),
                                        dateTimeUTCNow.hour() , dateTimeUTCNow.minute());
  Serial.println("");
  
  DateTime localTime = myTimezone.toLocal(dateTimeUTCNow.unixtime());
  
  Serial.printf("%s %i %i %i %i %i", (char *)"Local-Time is:", localTime.year(), 
                                        localTime.month() , localTime.day(),
                                        localTime.hour() , localTime.minute());
  Serial.println("");

}

void loop() 
{
  
  if (++loopCounter % 100000 == 0)   // Make decisions to send data every 100000 th round and toggle Led to signal that App is running
  {
    
    ledState = !ledState;
    digitalWriteFast(LED_BUILTIN, ledState);    // toggle LED to signal that App is running

    #if WORK_WITH_WATCHDOG == 1
      wdt.feed();
    #endif
    
      // Update RTC from Ntp when ntpUpdateInterval has expired, retry when RetryInterval has expired 
      
      if (timeClient.update())
      {                                                                  
        dateTimeUTCNow = sysTime.getTime();
        uint32_t actRtcTime = dateTimeUTCNow.secondstime();       
        unsigned long utcTime = timeClient.getUTCEpochTime();  // Seconds since 1. Jan. 1970    
        sysTime.setTime(utcTime + SECONDS_FROM_1970_TO_2000);
        dateTimeUTCNow = sysTime.getTime();
        sysTimeNtpDelta = actRtcTime - dateTimeUTCNow.secondstime();
        timeNtpUpdateCounter++;

        #if SERIAL_PRINT == 1
          // Indicate that NTP time was updated         
          char buffer[] = "NTP-Utc: YYYY-MM-DD hh:mm:ss";           
          dateTimeUTCNow.toString(buffer);
          Serial.println(buffer);
        #endif
      }  // End NTP stuff
       
      dateTimeUTCNow = sysTime.getTime();
      
      // Get offset in minutes between UTC and local time with consideration of DST
      int timeZoneOffsetUTC = myTimezone.utcIsDST(dateTimeUTCNow.unixtime()) ? TIMEZONEOFFSET + DSTOFFSET : TIMEZONEOFFSET;
      
      DateTime localTime = myTimezone.toLocal(dateTimeUTCNow.unixtime());

      // In the last 15 sec of each day we set a pulse to Off-State when we had On-State before
      bool isLast15SecondsOfDay = (localTime.hour() == 23 && localTime.minute() == 59 &&  localTime.second() > 45) ? true : false;
      
      // Get readings from 4 differend analog sensors and store the values in a container
      dataContainer.SetNewValue(0, dateTimeUTCNow, ReadAnalogSensor(0));
      dataContainer.SetNewValue(1, dateTimeUTCNow, ReadAnalogSensor(1));
      dataContainer.SetNewValue(2, dateTimeUTCNow, ReadAnalogSensor(2));
      dataContainer.SetNewValue(3, dateTimeUTCNow, ReadAnalogSensor(3));

      // Check if automatic OnOfSwitcher has toggled (used to simulate on/off changes)
      // and accordingly change the state of one representation (here index 0 and 1) in onOffDataContainer
      if (onOffSwitcherWio.hasToggled(dateTimeUTCNow))
      {
        bool state = onOffSwitcherWio.GetState();
        onOffDataContainer.SetNewOnOffValue(0, state, dateTimeUTCNow, timeZoneOffsetUTC);
        onOffDataContainer.SetNewOnOffValue(1, !state, dateTimeUTCNow, timeZoneOffsetUTC);
      }

      // Check if something is to do: send analog data ? send On/Off-Data ? Handle EndOfDay stuff ?
      if (dataContainer.hasToBeSent() || onOffDataContainer.One_hasToBeBeSent(localTime) || isLast15SecondsOfDay)
      {    
        //Create some buffer
        char sampleTime[25] {0};    // Buffer to hold sampletime        
        char strData[100];          // Buffer to hold display message  
        char EtagBuffer[50] {0};    // Buffer to hold returned Etag

        // Create az_span to hold partitionkey
        char partKeySpan[25] {0};
        size_t partitionKeyLength = 0;
        az_span partitionKey = AZ_SPAN_FROM_BUFFER(partKeySpan);
        
        // Create az_span to hold rowkey
        char rowKeySpan[25] {0};
        size_t rowKeyLength = 0;
        az_span rowKey = AZ_SPAN_FROM_BUFFER(rowKeySpan);

        if (dataContainer.hasToBeSent())       // have to send analog values ?
        {    
          // Retrieve edited sample values from container
          SampleValueSet sampleValueSet = dataContainer.getCheckedSampleValues(dateTimeUTCNow);
                  
          createSampleTime(sampleValueSet.LastUpdateTime, timeZoneOffsetUTC, (char *)sampleTime);

          // Define name of the table (arbitrary name + actual year, like: AnalogTestValues2020)
          String augmentedAnalogTableName = analogTableName; 
          if (augmentTableNameWithYear)
          {
            augmentedAnalogTableName += (dateTimeUTCNow.year());     
          }
          
          // Create Azure Storage Table if table doesn't exist
          if (localTime.year() != dataContainer.Year)    // if new year
          {  
            az_http_status_code theResult = createTable(myCloudStorageAccountPtr, (char *)augmentedAnalogTableName.c_str());
                     
            if ((theResult == AZ_HTTP_STATUS_CODE_CONFLICT) || (theResult == AZ_HTTP_STATUS_CODE_CREATED))
            {
              dataContainer.Set_Year(localTime.year());                   
            }
            else
            {
              // Reset board if not successful
              //https://forum.pjrc.com/threads/59935-Reboot-Teensy-programmatically?p=232143&viewfull=1#post232143
              //Reset Teensy 4.1
             SCB_AIRCR = 0x05FA0004;             
            }                     
          }
          

          // Create an Array of (here) 5 Properties
          // Each Property consists of the Name, the Value and the Type (here only Edm.String is supported)

          // Besides PartitionKey and RowKey we have 5 properties to be stored in a table row
          // (SampleTime and 4 samplevalues)
          size_t analogPropertyCount = 5;
          EntityProperty AnalogPropertiesArray[5];
          AnalogPropertiesArray[0] = (EntityProperty)TableEntityProperty((char *)"SampleTime", (char *) sampleTime, (char *)"Edm.String");
          AnalogPropertiesArray[1] = (EntityProperty)TableEntityProperty((char *)"T_1", (char *)floToStr(sampleValueSet.SampleValues[0].Value).c_str(), (char *)"Edm.String");
          AnalogPropertiesArray[2] = (EntityProperty)TableEntityProperty((char *)"T_2", (char *)floToStr(sampleValueSet.SampleValues[1].Value).c_str(), (char *)"Edm.String");
          AnalogPropertiesArray[3] = (EntityProperty)TableEntityProperty((char *)"T_3", (char *)floToStr(sampleValueSet.SampleValues[2].Value).c_str(), (char *)"Edm.String");
          AnalogPropertiesArray[4] = (EntityProperty)TableEntityProperty((char *)"T_4", (char *)floToStr(sampleValueSet.SampleValues[3].Value).c_str(), (char *)"Edm.String");
  
          // Create the PartitionKey (special format)
          makePartitionKey(analogTablePartPrefix, augmentPartitionKey, localTime, partitionKey, &partitionKeyLength);
          partitionKey = az_span_slice(partitionKey, 0, partitionKeyLength);

          // Create the RowKey (special format)        
          makeRowKey(localTime, rowKey, &rowKeyLength);
          
          rowKey = az_span_slice(rowKey, 0, rowKeyLength);
  
          // Create TableEntity consisting of PartitionKey, RowKey and the properties named 'SampleTime', 'T_1', 'T_2', 'T_3' and 'T_4'
          AnalogTableEntity analogTableEntity(partitionKey, rowKey, az_span_create_from_str((char *)sampleTime),  AnalogPropertiesArray, analogPropertyCount);
          
          #if SERIAL_PRINT == 1
          sprintf(strData, "   Trying to insert %u", insertCounterAnalogTable);
          Serial.println(strData);
          #endif  
             
          // Keep track of tries to insert and check for memory leak
          insertCounterAnalogTable++;

          // RoSchmi, Todo: event. include code to check for memory leaks here


          // Store Entity to Azure Cloud   
          __unused az_http_status_code insertResult =  insertTableEntity(myCloudStorageAccountPtr, (char *)augmentedAnalogTableName.c_str(), analogTableEntity, (char *)EtagBuffer);
                 
        }
        else     // Task to do was not send analog table, so it is Send On/Off values or End of day stuff?
        {
        
          OnOffSampleValueSet onOffValueSet = onOffDataContainer.GetOnOffValueSet();

          for (int i = 0; i < 4; i++)    // Do for 4 OnOff-Tables  
          {
            DateTime lastSwitchTimeDate = DateTime(onOffValueSet.OnOffSampleValues[i].LastSwitchTime.year(), 
                                                onOffValueSet.OnOffSampleValues[i].LastSwitchTime.month(), 
                                                onOffValueSet.OnOffSampleValues[i].LastSwitchTime.day());

            DateTime actTimeDate = DateTime(localTime.year(), localTime.month(), localTime.day());

            if (onOffValueSet.OnOffSampleValues[i].hasToBeSent || ((onOffValueSet.OnOffSampleValues[i].actState == true) &&  (lastSwitchTimeDate.operator!=(actTimeDate))))
            {
              onOffDataContainer.Reset_hasToBeSent(i);     
              EntityProperty OnOffPropertiesArray[5];

               // RoSchmi
               TimeSpan  onTime = onOffValueSet.OnOffSampleValues[i].OnTimeDay;
               if (lastSwitchTimeDate.operator!=(actTimeDate))
               {
                  onTime = TimeSpan(0);                 
                  onOffDataContainer.Set_OnTimeDay(i, onTime);

                  if (onOffValueSet.OnOffSampleValues[i].actState == true)
                  {
                    onOffDataContainer.Set_LastSwitchTime(i, actTimeDate);
                  }
               }
                          
              char OnTimeDay[15] = {0};
              sprintf(OnTimeDay, "%03i-%02i:%02i:%02i", onTime.days(), onTime.hours(), onTime.minutes(), onTime.seconds());
              createSampleTime(dateTimeUTCNow, timeZoneOffsetUTC, (char *)sampleTime);

              // Tablenames come from the onOffValueSet, here usually the tablename is augmented with the actual year
              String augmentedOnOffTableName = onOffValueSet.OnOffSampleValues[i].tableName;
              if (augmentTableNameWithYear)
              {               
                augmentedOnOffTableName += (localTime.year()); 
              }

              // Create table if table doesn't exist
              if (localTime.year() != onOffValueSet.OnOffSampleValues[i].Year)
              {
                 az_http_status_code theResult = createTable(myCloudStorageAccountPtr, (char *)augmentedOnOffTableName.c_str());
                 
                 if ((theResult == AZ_HTTP_STATUS_CODE_CONFLICT) || (theResult == AZ_HTTP_STATUS_CODE_CREATED))
                 {
                    onOffDataContainer.Set_Year(i, localTime.year());
                 }
                 else
                 {
                    delay(3000);
                     //Reset Teensy 4.1
                    SCB_AIRCR = 0x05FA0004;      
                 }
              }
              
              TimeSpan TimeFromLast = onOffValueSet.OnOffSampleValues[i].TimeFromLast;

              char timefromLast[15] = {0};
              sprintf(timefromLast, "%03i-%02i:%02i:%02i", TimeFromLast.days(), TimeFromLast.hours(), TimeFromLast.minutes(), TimeFromLast.seconds());
                         
              size_t onOffPropertyCount = 5;
              OnOffPropertiesArray[0] = (EntityProperty)TableEntityProperty((char *)"ActStatus", onOffValueSet.OnOffSampleValues[i].outInverter ? (char *)(onOffValueSet.OnOffSampleValues[i].actState ? "On" : "Off") : (char *)(onOffValueSet.OnOffSampleValues[i].actState ? "Off" : "On"), (char *)"Edm.String");
              OnOffPropertiesArray[1] = (EntityProperty)TableEntityProperty((char *)"LastStatus", onOffValueSet.OnOffSampleValues[i].outInverter ? (char *)(onOffValueSet.OnOffSampleValues[i].lastState ? "On" : "Off") : (char *)(onOffValueSet.OnOffSampleValues[i].lastState ? "Off" : "On"), (char *)"Edm.String");
              OnOffPropertiesArray[2] = (EntityProperty)TableEntityProperty((char *)"OnTimeDay", (char *) OnTimeDay, (char *)"Edm.String");
              OnOffPropertiesArray[3] = (EntityProperty)TableEntityProperty((char *)"SampleTime", (char *) sampleTime, (char *)"Edm.String");
              OnOffPropertiesArray[4] = (EntityProperty)TableEntityProperty((char *)"TimeFromLast", (char *) timefromLast, (char *)"Edm.String");
          
              // Create the PartitionKey (special format)
              makePartitionKey(onOffTablePartPrefix, augmentPartitionKey, localTime, partitionKey, &partitionKeyLength);
              partitionKey = az_span_slice(partitionKey, 0, partitionKeyLength);
              
              // Create the RowKey (special format)            
              makeRowKey(localTime, rowKey, &rowKeyLength);
              
              rowKey = az_span_slice(rowKey, 0, rowKeyLength);
  
              // Create TableEntity consisting of PartitionKey, RowKey and the properties named 'SampleTime', 'T_1', 'T_2', 'T_3' and 'T_4'
              OnOffTableEntity onOffTableEntity(partitionKey, rowKey, az_span_create_from_str((char *)sampleTime),  OnOffPropertiesArray, onOffPropertyCount);
          
              onOffValueSet.OnOffSampleValues[i].insertCounter++;
              
              // Store Entity to Azure Cloud   
             __unused az_http_status_code insertResult =  insertTableEntity(myCloudStorageAccountPtr, (char *)augmentedOnOffTableName.c_str(), onOffTableEntity, (char *)EtagBuffer);
              
              delay(1000);     // wait at least 1 sec so that two uploads cannot have the same RowKey

              break;          // Send only one in each round of loop 
            }
            else
            {
              if (isLast15SecondsOfDay && !onOffValueSet.OnOffSampleValues[i].dayIsLocked)
              {
                if (onOffValueSet.OnOffSampleValues[i].actState == true)              
                {               
                   onOffDataContainer.Set_ResetToOnIsNeededFlag(i, true);                 
                   onOffDataContainer.SetNewOnOffValue(i, onOffValueSet.OnOffSampleValues[i].inputInverter ? true : false, dateTimeUTCNow, timeZoneOffsetUTC);
                   delay(1000);   // because we don't want to send twice in the same second 
                  break;
                }
                else
                {              
                  if (onOffValueSet.OnOffSampleValues[i].resetToOnIsNeeded)
                  {                  
                    onOffDataContainer.Set_DayIsLockedFlag(i, true);
                    onOffDataContainer.Set_ResetToOnIsNeededFlag(i, false);
                    onOffDataContainer.SetNewOnOffValue(i, onOffValueSet.OnOffSampleValues[i].inputInverter ? false : true, dateTimeUTCNow, timeZoneOffsetUTC);
                    break;
                  }                 
                }              
              }
            }              
          }
          
        } 
      }    
     
  }
}         // End loop


// To manage daylightsavingstime stuff convert input ("Last", "First", "Second", "Third", "Fourth") to int equivalent
int getWeekOfMonthNum(const char * weekOfMonth)
{
  for (int i = 0; i < 5; i++)
  {  
    if (strcmp((char *)timeNameHelper.weekOfMonth[i], weekOfMonth) == 0)
    {
      return i;
    }   
  }
  return -1;
}

int getMonNum(const char * month)
{
  for (int i = 0; i < 12; i++)
  {  
    if (strcmp((char *)timeNameHelper.monthsOfTheYear[i], month) == 0)
    {
      return i + 1;
    }   
  }
  return -1;
}

int getDayNum(const char * day)
{
  for (int i = 0; i < 7; i++)
  {  
    if (strcmp((char *)timeNameHelper.daysOfTheWeek[i], day) == 0)
    {
      return i + 1;
    }   
  }
  return -1;
}

String floToStr(float value)
{
  char buf[10];
  sprintf(buf, "%.1f", (roundf(value * 10.0))/10.0);
  return String(buf);
}

float ReadAnalogSensor(int pSensorIndex)
{
#ifndef USE_SIMULATED_SENSORVALUES
            // Use values read from an analog source
            // Change the function for each sensor to your needs

            double theRead = MAGIC_NUMBER_INVALID;

            if (analogSensorMgr.HasToBeRead(pSensorIndex, dateTimeUTCNow))
            {                     
              switch (pSensorIndex)
              {
                case 0:
                    {
                        float temp_hum_val[2] = {0};
                        if (!dht.readTempAndHumidity(temp_hum_val))
                        {
                            analogSensorMgr.SetReadTimeAndValues(pSensorIndex, dateTimeUTCNow, temp_hum_val[1], temp_hum_val[0], MAGIC_NUMBER_INVALID);
                            
                            theRead = temp_hum_val[1];
                            // Take theRead (nearly) 0.0 as invalid
                            // (if no sensor is connected the function returns 0)                        
                            if (!(theRead > - 0.00001 && theRead < 0.00001))
                            {      
                                theRead += SENSOR_1_OFFSET;                                                       
                            }
                            else
                            {
                              theRead = MAGIC_NUMBER_INVALID;
                            }                            
                        }                                                           
                    }
                    break;

                case 1:
                    {
                      // Here we look if the temperature sensor was updated in this loop
                      // If yes, we can get the measured humidity value from the index 0 sensor
                      AnalogSensor tempSensor = analogSensorMgr.GetSensorDates(0);
                      if (tempSensor.LastReadTime.operator==(dateTimeUTCNow))
                      {
                          analogSensorMgr.SetReadTimeAndValues(pSensorIndex, dateTimeUTCNow, tempSensor.Value_1, tempSensor.Value_2, MAGIC_NUMBER_INVALID);
                          theRead = tempSensor.Value_2;
                            // Take theRead (nearly) 0.0 as invalid
                            // (if no sensor is connected the function returns 0)                        
                            if (!(theRead > - 0.00001 && theRead < 0.00001))
                            {      
                                theRead += SENSOR_2_OFFSET;                                                       
                            }
                            else
                            {
                              theRead = MAGIC_NUMBER_INVALID;
                            }                          
                      }                
                    }
                    break;
                case 2:
                    {
                        // Here we do not send a sensor value but the state of the upload counter
                        // Upload counter, limited to max. value of 1399
                        //theRead = (insertCounterAnalogTable % 1399) / 10.0 ;

                        // Alternative                  
                        
                        // Read the light sensor (not used here, collumn is used as upload counter)
                        theRead = analogRead(WIO_LIGHT);
                        theRead = map(theRead, 0, 1023, 0, 100);
                        theRead = theRead < 0 ? 0 : theRead > 100 ? 100 : theRead;
                                                                    
                    }
                    break;
                case 3:
                    /*                
                    {
                        // Here we do not send a sensor value but the last reset cause
                        // Read the last reset cause for dignostic purpose 
                        theRead = lastResetCause;                        
                    }
                    */

                    // Read the accelerometer (not used here)
                    // First experiments, don't work well
                    
                    {
                        ImuSampleValues sampleValues;
                        sampleValues.X_Read = lis.getAccelerationX();
                        sampleValues.Y_Read = lis.getAccelerationY();
                        sampleValues.Z_Read = lis.getAccelerationZ();
                        imuManagerWio.SetNewImuReadings(sampleValues);

                        theRead = imuManagerWio.GetVibrationValue();                                                                 
                    } 
                    
                    break;
              }
            }          
            return theRead ;
#endif

#ifdef USE_SIMULATED_SENSORVALUES
      #ifdef USE_TEST_VALUES
            // Here you can select that diagnostic values (for debugging)
            // are sent to your storage table
            double theRead = MAGIC_NUMBER_INVALID;
            switch (pSensorIndex)
            {
                case 0:
                    {
                        theRead = timeNtpUpdateCounter;
                        theRead = theRead / 10; 
                    }
                    break;

                case 1:
                    {                       
                        theRead = sysTimeNtpDelta > 140 ? 140 : sysTimeNtpDelta < - 40 ? -40 : (double)sysTimeNtpDelta;                      
                    }
                    break;
                case 2:
                    {
                        theRead = insertCounterAnalogTable;
                        theRead = theRead / 10;                      
                    }
                    break;
                case 3:
                    {
                        theRead = lastResetCause;                       
                    }
                    break;
            }

            return theRead ;

  

        #endif
            
            onOffSwitcherWio.SetActive();
            // Only as an example we here return values which draw a sinus curve            
            int frequDeterminer = 4;
            int y_offset = 1;
            // different frequency and y_offset for aIn_0 to aIn_3
            if (pSensorIndex == 0)
            { frequDeterminer = 4; y_offset = 1; }
            if (pSensorIndex == 1)
            { frequDeterminer = 8; y_offset = 10; }
            if (pSensorIndex == 2)
            { frequDeterminer = 12; y_offset = 20; }
            if (pSensorIndex == 3)
            { frequDeterminer = 16; y_offset = 30; }
             
            int secondsOnDayElapsed = dateTimeUTCNow.second() + dateTimeUTCNow.minute() * 60 + dateTimeUTCNow.hour() *60 *60;

            // RoSchmi
            switch (pSensorIndex)
            {
              case 3:
              {
                return lastResetCause;
              }
              break;
            
              case 2:
              { 
                uint32_t showInsertCounter = insertCounterAnalogTable % 50;               
                double theRead = ((double)showInsertCounter) / 10;
                return theRead;
              }
              break;
              case 0:
              case 1:
              {
                return roundf((float)25.0 * (float)sin(PI / 2.0 + (secondsOnDayElapsed * ((frequDeterminer * PI) / (float)86400)))) / 10  + y_offset;          
              }
              break;
              default:
              {
                return 0;
              }
            }
  #endif
}

void createSampleTime(DateTime dateTimeUTCNow, int timeZoneOffsetUTC, char * sampleTime)
{
  int hoursOffset = timeZoneOffsetUTC / 60;
  int minutesOffset = timeZoneOffsetUTC % 60;
  char sign = timeZoneOffsetUTC < 0 ? '-' : '+';
  char TimeOffsetUTCString[10];
  sprintf(TimeOffsetUTCString, " %c%03i", sign, timeZoneOffsetUTC);
  TimeSpan timespanOffsetToUTC = TimeSpan(0, hoursOffset, minutesOffset, 0);
  DateTime newDateTime = dateTimeUTCNow + timespanOffsetToUTC;
  sprintf(sampleTime, "%02i/%02i/%04i %02i:%02i:%02i%s",newDateTime.month(), newDateTime.day(), newDateTime.year(), newDateTime.hour(), newDateTime.minute(), newDateTime.second(), TimeOffsetUTCString);
}
 
void makeRowKey(DateTime actDate,  az_span outSpan, size_t *outSpanLength)
{
  // formatting the RowKey (= reverseDate) this way to have the tables sorted with last added row upmost
  char rowKeyBuf[20] {0};

  sprintf(rowKeyBuf, "%4i%02i%02i%02i%02i%02i", (10000 - actDate.year()), (12 - actDate.month()), (31 - actDate.day()), (23 - actDate.hour()), (59 - actDate.minute()), (59 - actDate.second()));
  az_span retValue = az_span_create_from_str((char *)rowKeyBuf);
  az_span_copy(outSpan, retValue);
  *outSpanLength = retValue._internal.size;         
}

void makePartitionKey(const char * partitionKeyprefix, bool augmentWithYear, DateTime dateTime, az_span outSpan, size_t *outSpanLength)
{
  // if wanted, augment with year and month (12 - month for right order)                    
  char dateBuf[20] {0};
  sprintf(dateBuf, "%s%d-%02d", partitionKeyprefix, (dateTime.year()), (12 - dateTime.month()));                  
  az_span ret_1 = az_span_create_from_str((char *)dateBuf);
  az_span ret_2 = az_span_create_from_str((char *)partitionKeyprefix);                       
  if (augmentWithYear == true)
  {
    az_span_copy(outSpan, ret_1);            
    *outSpanLength = ret_1._internal.size; 
  }
    else
  {
    az_span_copy(outSpan, ret_2);
    *outSpanLength = ret_2._internal.size;
  }    
}


az_http_status_code insertTableEntity(CloudStorageAccount *pAccountPtr, const char * pTableName, TableEntity pTableEntity, char * outInsertETag)
{
  static EthernetClient  client;
  //static EthernetSSLClient sslClient(client, TAs, (size_t)TAs_NUM, 1, EthernetSSLClient::SSL_DUMP);   // Define Log Level
  static EthernetSSLClient sslClient(client, TAs, (size_t)TAs_NUM);
  
  #if TRANSPORT_PROTOCOL == 1    
    EthernetHttpClient  httpClient(sslClient, pAccountPtr->HostNameTable, (TRANSPORT_PROTOCOL == 0) ? 80 : 443);
  #else
    EthernetHttpClient  httpClient(client, pAccountPtr->HostNameTable, (TRANSPORT_PROTOCOL == 0) ? 80 : 443);
  #endif
  
  TableClient table(pAccountPtr, (TRANSPORT_PROTOCOL == 0) ? Protocol::useHttp : Protocol::useHttps, TAs[(size_t)TAs_NUM], (size_t)TAs_NUM, &client, &sslClient, &httpClient);

  #if WORK_WITH_WATCHDOG == 1
      wdt.feed();
  #endif
  
  DateTime responseHeaderDateTime = DateTime();   // Will be filled with DateTime value of the response from Azure Service

  // Insert Entity
  az_http_status_code statusCode = table.InsertTableEntity(pTableName, pTableEntity, (char *)outInsertETag, &responseHeaderDateTime, ContType::contApplicationIatomIxml, AcceptType::acceptApplicationIjson, ResponseType::dont_returnContent, false);
  
  #if WORK_WITH_WATCHDOG == 1
      wdt.feed();
  #endif

  lastResetCause = 0;
  tryUploadCounter++;

   // RoSchmi for tests: to simulate failed upload
  //az_http_status_code   statusCode = AZ_HTTP_STATUS_CODE_UNAUTHORIZED;
  
  if ((statusCode == AZ_HTTP_STATUS_CODE_NO_CONTENT) || (statusCode == AZ_HTTP_STATUS_CODE_CREATED))
  {
    sendResultState = true;
    
    #if SERIAL_PRINT == 1
    Serial.println(F("InsertRequest: Entity was inserted"));
    #endif
    
    #if UPDATE_TIME_FROM_AZURE_RESPONSE == 1    // System time shall be updated from the DateTime value of the response ?
    
      dateTimeUTCNow = sysTime.getTime();    
      uint32_t actRtcTime = dateTimeUTCNow.secondstime();
      dateTimeUTCNow = responseHeaderDateTime;                    // Get new time from the response
      unsigned long utcTime = timeClient.getUTCEpochTime();                     // Seconds since 1. Jan. 1970
      sysTime.setTime(utcTime + SECONDS_FROM_1970_TO_2000);       // actualize SystemTime (RTC)
      dateTimeUTCNow = sysTime.getTime();                         // actualize variable 'dateTimeUTCNow' from RTC
      sysTimeNtpDelta = actRtcTime - dateTimeUTCNow.secondstime();// calculate the time deviation since the last actualization

      #if SERIAL_PRINT == 1
      char buffer[] = "Azure-Utc: YYYY-MM-DD hh:mm:ss";
      dateTimeUTCNow.toString(buffer);
      Serial.println(buffer);
      #endif   
    #endif
  }
  else            // request failed
  {               // note: ugly hack!! internal error codes from -1 to -4 were converted for tests to error codes 401 to 404 since
                  // negative values cannot be returned as 'az_http_status_code' 

    failedUploadCounter++;
    sendResultState = false;
    lastResetCause = 100;      // Set lastResetCause to arbitrary value of 100 to signal that post request failed
    
    Serial.println(F("InsertRequest: failed ******************"));
    
    #if REBOOT_AFTER_FAILED_UPLOAD == 1   // When selected in config.h -> Reboot through SystemReset after second failed upload

          //https://forum.pjrc.com/threads/59935-Reboot-Teensy-programmatically?p=232143&viewfull=1#post232143

          if(failedUploadCounter > 1)
          {
            // Reset Teensy 4.1
            SCB_AIRCR = 0x05FA0004;        
          }
       
    #endif
    
    #if WORK_WITH_WATCHDOG == 1
      wdt.feed();  
    #endif

    delay(1000);
  }
  return statusCode;
}

az_http_status_code createTable(CloudStorageAccount *pAccountPtr, const char * pTableName)
{ 
  #if SERIAL_PRINT == 1
  Serial.println("Trying to create Table");
  #endif

  static EthernetClient  client;
  
  //static EthernetSSLClient sslClient(client, TAs, (size_t)TAs_NUM, 1, EthernetSSLClient::SSL_DUMP);   // Define Log Level
  static EthernetSSLClient sslClient(client, TAs, (size_t)TAs_NUM);

  #if TRANSPORT_PROTOCOL == 1
    EthernetHttpClient  httpClient(sslClient, pAccountPtr->HostNameTable, (TRANSPORT_PROTOCOL == 0) ? 80 : 443);
  #else
    EthernetHttpClient  httpClient(client, pAccountPtr->HostNameTable, (TRANSPORT_PROTOCOL == 0) ? 80 : 443);
  #endif
  
  #if WORK_WITH_WATCHDOG == 1
      wdt.feed();
  #endif
  
  TableClient table(pAccountPtr, Protocol::useHttps, TAs[(size_t)TAs_NUM], (size_t)TAs_NUM, &client, &sslClient, &httpClient);
  
  // Create Table
  az_http_status_code statusCode = table.CreateTable(pTableName, ContType::contApplicationIatomIxml, AcceptType::acceptApplicationIjson, ResponseType::dont_returnContent, false);

   // RoSchmi for tests: to simulate failed upload
  //az_http_status_code   statusCode = AZ_HTTP_STATUS_CODE_UNAUTHORIZED;

  if ((statusCode == AZ_HTTP_STATUS_CODE_CONFLICT) || (statusCode == AZ_HTTP_STATUS_CODE_CREATED))
  {
    #if WORK_WITH_WATCHDOG == 1
      wdt.feed();
    #endif
    
    #if SERIAL_PRINT == 1
    Serial.println("Table is available");
    #endif   
  }
  else
  {
    
    Serial.println("Table Creation failed: ");
    delay(1000);
    // Reset Teensy 4.1
    SCB_AIRCR = 0x05FA0004;
    
  }
  delay(1000);
  return statusCode;
}

