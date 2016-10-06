import datetime
import os
import re
from collections import defaultdict

class RINEXData():
    def __init__(self, local_file):
        """Initialize RINEXData object

        Creates RINEXData object when passed a local RINEX filename - then 
        parses the file name and RINEX header to extract variables

        If file is not parsable, then an exception is raised on initialization

        Input:
            local_file  Local path to dowloaded RINEX file

        Attributes generated:
            file_name   Basename of input file
            file_data   Binary contents of input file
        """
        self.local_file = local_file
        self.file_name = os.path.basename(local_file)

        self._parseFilename()

        with open(local_file, 'rb') as file_data:
            self.file_data = file_data.read()

        self._parseHeader()

    def _parseFilename(self):
        """Extract variables from filename and assert syntax

        This should only fail if the regex does not match, so the only
        exception which this can raise is an InvalidFilename

        Variables generated:
            data_type   Type of data, string containing 'N', 'O', or 'M' for
                        Navigation, Observation, or Meteorological respectively

            file_type   String containing 'daily', 'hourly', or 'highrate'
                        representing the duration of the file

            start_time  Starting UTC date and time of file data: YYYYDDDHHMM,
                        datetime object

            marker_name Four character site identifier

        Example filenames:
            Long name
                ALIC00AUS_R_20161280000_01D_30S_MO.rnx
                EDSV00AUS_R_20161280000_01D_EN.rnx
                    Note: sample rate ('30S' above) is variably included

            Short name
                bula1280.16d        Daily specifies hours as integer
                alby028g.16n        Hourly specifies hours as alpha (a-z, 0-23)
                ALBY124V00.16d      Highrate has alpha hours and specifies 
                                    minutes
        """
        assert self.file_name
        filename = self.file_name.lower()

        # Define RINEX long and short name regex patterns
        rinex_longname_pattern = re.compile(
            '^[a-z\d]{9}_r_\d{11}_\d\d[dhm]_'
            '(\d\d[a-z]_)?[megjr][mon]\.[cr][rn]x$',
            re.IGNORECASE)

        rinex_shortname_pattern = re.compile(
            '^[a-z\d]{4}\d{3}[a-x\d](\d\d)?\.\d\d[gndmo]$',
            re.IGNORECASE)

        # If the regex matches, we can safely extract variables from filename    
        if re.match(rinex_longname_pattern, filename):
            # RINEX longname
            # Remove crx/rnx extension and split name on underscore delimeter
            filename, extension = os.path.splitext(filename)
            split_name = filename.split('_')

            # First four characters of filename contain marker name
            self.marker_name = filename[0:4].upper()

            # Last character of name defines data type (o, m, or n)
            self.data_type = str(split_name[-1][-1])

            # filetype is assumed based on the unit of time given for range
            # Days = daily, Hours = hourly, Minutes = highrate
            file_types = {'d': 'daily', 'h': 'hourly', 'm': 'highrate'}
            self.file_type = file_types[split_name[3][-1]]

            # Date and time are syntactially correct in longname
            date_time = split_name[2]
            self.start_time = datetime.datetime.strptime(date_time, '%Y%j%H%M')

        elif re.match(rinex_shortname_pattern, filename):
            # RINEX shortname
            # First four characters of filename contain marker name
            self.marker_name = filename[0:4].upper()
            # Last character of filename defines data type
            data_type = filename[-1]
            # GLONASS Navigation file
            if data_type == 'g': data_type = 'n'
            # Hatanaka compressed observation file
            if data_type == 'd': data_type = 'o'

            self.data_type = str(data_type)

            # Hour and minute are 0 by default
            hour, minute = 0, 0

            # Filetype is assumed based on hours and whether or not minutes are
            # present: Daily if hours is 0-9, hourly if minutes are not given, 
            # otherwise highrate
            check_hour = filename[7:9]
            if re.match('^\d$', check_hour[0]):
                file_type = 'daily'
            else:
                # Get hour for highrate and hourly data
                # Convert alpha-hour to integer, a - x = 0 - 23
                hour = int(ord(check_hour[0]) - 97)
                if check_hour[1] == '.':
                    file_type = 'hourly'
                else:
                    file_type = 'highrate'
                    # Get minutes for highrate data
                    minute = int(filename[8:10])

            self.file_type = file_type

            # Year and day are given uniformly for all file types in shortname
            # Day is 3 digit day of year
            day = int(filename[4:7])
            # Year is given as 2 digits
            year = int(filename.split('.')[-1][0:2])

            self.start_time = datetime.datetime.strptime(
                '{}-{}-{}-{}'.format(year, day, hour, minute), '%y-%j-%H-%M')

        else:
            # Invalid filename did not match regex for RINEX long or short name
            err = 'Filename does not match RINEX formatting: {}'.format(
                filename)
            raise InvalidFilename(err)

        return True

    def _parseHeader(self):
        """Extract variables from RINEX header and assert syntax

        Returns True if header is parsed successfully, otherwise raises error

        Variables generated:
            Observation, Navigational, and Meteorological files -
                marker_number       site DOMES number
                version             RINEX version
                
            Observation files -
                receiver_number     serial number of receiver
                receiver_type       type of receiver
                receiver_version    receiver firmware version
                antenna_number      serial number of antenna
                antenna_type        type of antenna
                antenna_height      height of antenna
                antenna_east        antenna delta position east
                antenna_north       antenna delta position north
                
            Meteorological files -
                sensor_type         type of meteorological sensor
                sensor_height       height of sensor
        """
        assert self.file_data
        
        # Extract header fields from filedata - check header syntax
        try:
            header, eoh, self.observations = self.file_data.partition('END OF HEADER')
            if not eoh:
                raise RINEXHeaderMissingField('Missing END OF HEADER field')

            self.header = header + eoh

            # Function to remove whitespace from string
            trim_whitespace = lambda a: ' '.join(str(a).split())

            # Split header fields into dictionary, where the field label is the key
            header_fields = defaultdict(str)
            for line in self.header.splitlines():
                field, label = line[:60], trim_whitespace(line[60:])
                header_fields[label] += field
            header_fields = dict(header_fields)

            # A ValueError will be raised if any fields cannot be converted to
            # the correct type
            # Extract fields and remove excess whitespace
            if self.data_type == 'o':
                # Extract relevant fields from Observational data
                receiver_info = header_fields['REC # / TYPE / VERS']
                self.receiver_number = trim_whitespace(receiver_info[0:20])
                self.receiver_type = trim_whitespace(receiver_info[20:40])
                self.receiver_version = trim_whitespace(receiver_info[40:60])
                
                antenna_info = header_fields['ANT # / TYPE']
                self.antenna_number = trim_whitespace(antenna_info[0:20])
                self.antenna_type = str(antenna_info[20:40])
                
                antenna_delta = header_fields['ANTENNA: DELTA H/E/N']
                self.antenna_height = float(antenna_delta[0:14])
                self.antenna_east = float(antenna_delta[14:28])
                self.antenna_north = float(antenna_delta[28:42])

            elif self.data_type == 'm':
                # Extract relevant fields from Meteorological data
                self.sensor_type = trim_whitespace(
                    header_fields['SENSOR MOD/TYPE/ACC'][0:40])
                
                self.sensor_height = float(
                    header_fields['SENSOR POS XYZ/H'][42:56])

            if self.data_type in ['o', 'm']:
                # Marker number is present in Observation and Meteorological data
                self.marker_number = trim_whitespace(
                    header_fields['MARKER NUMBER'][0:20])

            # RINEX version is present in all data
            self.version = float(header_fields['RINEX VERSION / TYPE'][0:20])

        except KeyError:
            err = 'Header is syntactially incorrect or missing fields'
            raise RINEXHeaderMissingField(err)
            
        except ValueError:
            err = 'Could not convert field found in header to expected type'
            raise RINEXHeaderFieldError(err)

        return True

# Custom Exceptions
class InvalidFilename(Exception):
    """Used to indicate that a filename is in some way not valid"""

class RINEXHeaderMissingField(Exception):
    """Used to indicate that a RINEX header is missing required data fields"""

class RINEXHeaderFieldError(ValueError):
    """Used to indicate that a RINEX header field was of the wrong data type
    
    Example:
        If a string is found where a float was expected
    """
