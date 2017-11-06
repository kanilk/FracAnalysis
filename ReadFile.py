
"""
    __author__ : Anil Kumar Kuppa

"""

import luigi
import h5py
import pandas as pd
import numpy as np
import os


# config classes

# this class extends the Luigi base class Config
# class channelListConfig(luigi.Config):
#     # @Param
#     channelList = luigi.ListParameter()
#

# Global parameters - configurable global parameters
PUMP_IDLE_SPEED = 10.0
ENG_ON_SPEED = 700.0

# Tasks
# this class extends the Luigi base class Task
# because we are simply pulling in an external file


class readFile(luigi.Task):
    """
        Define the input file for our job:
            The output method of this class returns the .csv file with the selected channels

        Parameter definition: input file path:
            Pass path to the .GH5 file to the readFile task as a parameter
        @Param1 - path to the .GH5 file, example: "E:/CAT_FrackingStageAnalysis/PEMS_Project_Invati_Cyient/R1S01590_VIMS"
        @Param2 - filename, example: "R1S01590170916100447_DQ1-81.DL3.GH5"

    """

    param1 = luigi.Parameter()
    param2 = luigi.Parameter()

    #fName = os.path.join(self.param1, param2)

    # param1 = "E:/CAT_FrackingStageAnalysis/PEMS_Project_Invati_Cyient/R1S01590_VIMS"
    # param2 = "R1S01590170916100447_DQ1-81.DL3.GH5"
    # fName = os.path.join(param1, param2)

    # Pass channel list to the readFile task as a parameter

    #cList = list(channelListConfig().channelList)

    # os.listdir("E:/CAT_FrackingStageAnalysis/PEMS_Project_Invati_Cyient/R1S01590_VIMS")
    # os.path.join(Param1,Param2)

    def run(self):

        channel_list = ['Engine Load Factor-Engine(65581.0)', 'Engine Speed-Engine(65536.0)',
                        'Fuel Consumption Rate-Engine(65714.0)', 'GLOBAL_TIME',
                        'Transmission Current Gear-CAN1 Device#3(523.0)',
                        'Well Stimulation Pump Intake Pressure-Pump Monitor(73495.0)',
                        'Well Stimulation Pump Oil Pressure-Pump Monitor(73499.0)',
                        'Well Stimulation Pump Oil Temperature-Pump Monitor(73500.0)',
                        'Well Stimulation Pump Outlet Pressure-Pump Monitor(73496.0)',
                        'Well Stimulation Pump Speed-Pump Monitor(73497.0)']

        # Pull the Asset / Machine Ser No. from the file name
        #assetSerNo = self.param2.rsplit('.', 2)[0]
        assetSerNo = self.param2[0:8]
        print("********" + str(assetSerNo) + "**************")

        fName = os.path.join(self.param1, self.param2)
        # create a dictionary of data frames
        df_all = []
        with h5py.File(fName, "r+") as hf:
            print("********" + str(fName) + "**************")
            cName0 = 'DYNAMIC DATA/PUMP_STANDARD/DOWNLOAD INFO/TIME/TIME VECTORS/'
            #cName1 = 'GLOBAL_TIME'
            cName2 = '/Measured'
            for channel in channel_list:
                #cName1 = channel_list[3]
                cName1 = channel
                # cName1 = cList[3]
                cName = cName0 + cName1 + cName2
                print("********" + str(cName) + "**************")
                data = pd.DataFrame(np.array(hf[cName][:]))
                print(" *********** GH5 file read successfully ********")
                print("********" + str(data.shape) + "**************")
                # Or use the `len()` function with the `index` property
                print("********" + str(len(data.index)) + "**************")
                print("**********************************************************")
                print(data.head(2))
                df_all.append(data)
            result = pd.concat(df_all,axis=1, ignore_index=True)
            result.columns = channel_list
            result['MACHSERNO'] = assetSerNo
            print("********" + str(result.shape) + "**************")
            print(result.columns.get_values().tolist())
            print(result.head(2))

            # convert time
            # result['global_time_utc'] = result['GLOBAL_TIME'].
            #result['global_time_utc'] = pd.to_datetime(result['GLOBAL_TIME'], format='%d%b%Y:%H:%M:%S.%f')
            print("result data post time conversion")
            print(result.head(2))

            hf.close()
        # convert pandas data frame to .csv file to save onto local disk using output() method
        with self.output().open('w') as out_file:
            res_csv = result.to_csv(index=False)
            out_file.write(res_csv)


    def output(self):
        return luigi.LocalTarget("C:/Users/User/AppData/Roaming/Microsoft/Windows/Start Menu/Programs/Anaconda3 (64-bit)/sampleScripts/tempRes/readFile_result.csv")


class fuelConsumptionVsPumpIdle(luigi.Task):

    """

                this method computes the fuel consumption when the pump is idle/non-idle time ; pumping time

                arguments:
                    @param p1: path to the directory
                    @param p2: file name
                    @param clist: channel list provided
                    @param date: date field
                    @param pumpIdleSpeed: indicates pump is idle or not (example, Pump Speed <= 10 rpm)
                    @param engOnSpeed: indicates engine is running or not (example, Eng Speed >= 720 rpm)

                return:
                    pandas data frame with the calculations
    """

    param1 = luigi.Parameter()
    param2 = luigi.Parameter()

    # Fuel Consumption vs Pump Idle
    @staticmethod
    def checkPumpIdle(x, y,):
        if (x <= PUMP_IDLE_SPEED) & (y >= ENG_ON_SPEED):
            return 1
        else:
            return 0

    def requires(self):
        return readFile(param1=self.param1, param2=self.param2)

    def run(self):

        with self.input().open('r') as infile:
            #adframe = pd.DataFrame(infile.read())
            #adframe = infile.read()
            adframe = pd.read_csv(infile)
            print("******** do Something task *********")
            #print("********" + str(adframe) + "**************")

            # check for pump Idle condition
            adframe['pumpIdle'] = adframe.apply(lambda row: self.checkPumpIdle(x=row['Well Stimulation Pump Speed-Pump Monitor(73497.0)'],
                                                                               y=row['Engine Speed-Engine(65536.0)']),
                                                                               axis=1)

            print("******" + str(len(adframe)) + "*****")
            print("******" + str(len(adframe[adframe['pumpIdle'] == 1])) + "*****")
            print("******" + str(len(adframe[adframe['pumpIdle'] == 0])) + "*****")

        # convert pandas data frame to .csv file to save onto local disk using output() method
        with self.output().open('w') as out_file:
            out_file.write(adframe.to_csv(index=False))

    def output(self):
        return luigi.LocalTarget("C:/Users/User/AppData/Roaming/Microsoft/Windows/Start Menu/Programs/Anaconda3 (64-bit)/sampleScripts/tempRes/Fuel_Vs_PumpIdle_result.csv")



if __name__ == '__main__':
    luigi.run()

