# AzureDataSender_Teensy_QnEthernet

Sending Sensor Data to Azure Storage Tables using Teensy 4.1.
Works with http- and https- transmission.

In this Repo the new 'QnEthernet' library is used
There is another version avaialble which uses the NativeEthernet and FNET libraries
https://github.com/RoSchmi/AzureDataSender_Teensy

For details about the functions of this app and how to use and apply settings have a look on this similar project

https://www.hackster.io/RoSchmi/wio-terminal-app-sending-sensor-data-to-azure-storage-tables-dbb08e

### Patches for Teensy 4.1 board
There is a hack needed to achieve that this application works on the Teensy 4.1:
1) The used Stream.h header file has to be changed:
   Go to C:/Users/thisUser/.platformio/packages/framework-arduinoteensy/cores/teensy4/Stream.h
   and exchange the Stream.h file with the Stream.h file found in the folder /lib/RoSchmi/Stream/ of this repository
   

   
  
