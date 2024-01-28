/* templated code goes here*/;

/* -----------------------------------------------------------------------------------------* 
   Vector Databases - Hydrate Chroma DB Collection

   Version: 1.0 (24JAN2024)
   Created: Sundaresh Sankaran(sundaresh.sankaran@sas.com)
    
   Available at: 
   https://github.com/SundareshSankaran/Vector-Databases---Hydrate-Chroma-DB-Collection
*------------------------------------------------------------------------------------------ */

/*-----------------------------------------------------------------------------------------*
   Python Block Definition
*------------------------------------------------------------------------------------------*/

/*-----------------------------------------------------------------------------------------*
   The following block of code has been created for the purpose of allowing proc python 
   to execute within a macro. Execution within a macro allows for other checks to be carried 
   out through SAS prior to handing off to the Python step.

   In this example, a temporary file is created containing the requisite Python commands, which 
   are then executed through infile reference.

   Note that Python code is pasted as-is and may be out of line with the SAS indentation followed.

*------------------------------------------------------------------------------------------*/
filename hcdccode temp;

data abc;

   infile datalines4 truncover pad;
   input ;   
   file hcdccode;
   put @1 _infile_;
   datalines4;

#############################################################################################
#
#  The pysqlite3 import allows for the code to also run in (some) older Python (or sqlite) versions.
#
#############################################################################################

__import__('pysqlite3')
import sys
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

#############################################################################################
#
#  Connect to CAS through the swat package
#
#############################################################################################

cas_session_exists =  SAS.symget("casSessionExists")
sessuuid        =  SAS.symget("_current_uuid_")
cas_host_path      =  SAS.symget("casHostPath")
cas_host_port      =  SAS.symget("casHostPort")

#############################################################################################
#
#  Add certificate location to operating system list of trusted certs detailed in 
#  About tab - Documentation 
#
#############################################################################################

import swat, os
os.environ['CAS_CLIENT_SSL_CA_LIST']=os.environ['SSLCALISTLOC']
SAS.logMessage("Session UUID is {}".format(sessuuid))   
conn            =  swat.CAS(hostname=cas_host_path,port=cas_host_port, password=os.environ['SAS_SERVICES_TOKEN'],session=sessuuid)

SAS.logMessage("Connection made.")   

#############################################################################################
#
#  Import chromadb
#
#############################################################################################

import chromadb

chroma_client = chromadb.Client()

print(chroma_client.heartbeat())

collection_name      =  SAS.symget("collectionName")

collection = chroma_client.get_or_create_collection(name=collection_name)

SAS.logMessage("Collection created: {}".format(collection.count()))

#############################################################################################
#
#  Import chromadb
#
#############################################################################################

input_table = SAS.symget("inputTable_name_base")
input_table_lib = SAS.symget("inputCaslib")

import re
scoredTable = conn.CASTable(name=input_table, caslib=input_table_lib)
# columnlist = list(filter(lambda x: re.search(r'_Col', x), scoredTable.columns.to_list()))

#############################################################################################
#
#  Create list of Embeddings based on the pattern column
#
#############################################################################################

embedding_pattern = SAS.symget("embeddingPattern")

import pandas
df = pandas.DataFrame()
df['Embeddings'] = (
    scoredTable.to_frame().filter(like=embedding_pattern)
      .apply(lambda row: row.dropna().tolist(), axis=1)
)

#############################################################################################
#
#  Embed and store the data
#
#############################################################################################

document_id = SAS.symget(docId)
text_variable = SAS.symget(textVar)
metadata_column = SAS.symget(metadataColumn)

if metadata_column:

   insertion = collection.add(
      ids=[str(i) for i in scoredTable[document_id]],  
      documents=[doc for doc in scoredTable[text_variable]],
      embeddings=[embedding for embedding in df["Embeddings"]],
      metadatas=[{"rating": target} for target in scoredTable["Target_Rating"]],
   )

else:

   insertion = collection.add(
      ids=[str(i) for i in scoredTable[document_id]],  
      documents=[doc for doc in scoredTable[text_variable]],
      embeddings=[embedding for embedding in df["Embeddings"]]
   )


SAS.logMessage("The collection contains {} documents.".format(collection.count()))

;;;;
   

run;

/*-----------------------------------------------------------------------------------------*
   START MACRO DEFINITIONS.
*------------------------------------------------------------------------------------------*/

/* -----------------------------------------------------------------------------------------* 
   Macro to create an error flag for capture during code execution.

   Input:
      1. errorFlagName: The name of the error flag you wish to create. Ensure you provide a 
         unique value to this parameter since it will be declared as a global variable.

    Output:
      2. &errorFlagName : A global variable which takes the name provided to errorFlagName.

   Also available at: 
   https://github.com/SundareshSankaran/sas_utility_programs/blob/main/code/Error%20Flag%20Creation/macro_create_error_flag.sas
*------------------------------------------------------------------------------------------ */


%macro _create_error_flag(errorFlagName);

   %global &errorFlagName.;
   %let  &errorFlagName.=0;

%mend _create_error_flag;



/* -------------------------------------------------------------------------------------------* 
   Macro to initialize a run-time trigger global macro variable to run SAS Studio Custom Steps. 
   A value of 1 (the default) enables this custom step to run.  A value of 0 (provided by 
   upstream code) sets this to disabled.

   Input:
   1. triggerName: The name of the runtime trigger you wish to create. Ensure you provide a 
      unique value to this parameter since it will be declared as a global variable.

   Output:
   2. &triggerName : A global variable which takes the name provided to triggerName.
   
   Also available at:
   https://github.com/SundareshSankaran/sas_utility_programs/blob/main/code/Create_Run_Time_Trigger/macro_create_runtime_trigger.sas
*-------------------------------------------------------------------------------------------- */

%macro _create_runtime_trigger(triggerName);

   %global &triggerName.;

   %if %sysevalf(%superq(&triggerName.)=, boolean)  %then %do;
  
      %put NOTE: Trigger macro variable &triggerName. does not exist. Creating it now.;
      %let &triggerName.=1;

   %end;

%mend _create_runtime_trigger;


/*-----------------------------------------------------------------------------------------*
   Macro variable to capture indicator of a currently active CAS session
*------------------------------------------------------------------------------------------*/

%global casSessionExists;
%global _current_uuid_;


/*-----------------------------------------------------------------------------------------*
   Macro to capture indicator and UUIDof any currently active CAS session.
   UUID is not expensive and can be used in future to consider graceful reconnect.

   Input:
   1. errorFlagName: name of an error flag that gets populated in case the connection is 
                     not active. Provide this value in quotes when executing the macro.
                     Define this as a global macro variable in order to use downstream.
   
   Output:
   1. Informational note as required. We explicitly don't provide an error note since 
      there is an easy recourse(of being able to connect to CAS)
   2. UUID of the session: macro variable which gets created if a session exists.

   Also available at: https://raw.githubusercontent.com/SundareshSankaran/sas_utility_programs/main/code/Check_For_Python/macro_python_check.sas
*------------------------------------------------------------------------------------------*/

%macro _env_cas_checkSession(errorFlagName);
    %if %sysfunc(symexist(_current_uuid_)) %then %do;
       %symdel _current_uuid_;
    %end;
    %if %sysfunc(symexist(_SESSREF_)) %then %do;
      %let casSessionExists= %sysfunc(sessfound(&_SESSREF_.));
      %if &casSessionExists.=1 %then %do;
         %global _current_uuid_;
         %let _current_uuid_=;   
         proc cas;
            session.sessionId result = sessresults;
            call symputx("_current_uuid_", sessresults[1]);
         quit;
         %put NOTE: A CAS session &_SESSREF_. is currently active with UUID &_current_uuid_. ;
      %end;
      %else %do;
         %put NOTE: Unable to find a currently active CAS session ;
         data _null_;
            call symputx(&errorFlagName., 1);
        run;
      %end;
   %end;
   %else %do;
      %put NOTE: No active CAS session ;
      data _null_;
        call symputx(&errorFlagName., 1);
      run;
   %end;
%mend _env_cas_checkSession;


/*-----------------------------------------------------------------------------------------*
   This macro creates a global macro variable called _usr_nameCaslib
   that contains the caslib name (aka. caslib-reference-name) associated with the libname 
   and assumes that the libname is using the CAS engine.

   As sysvalue has a length of 1024 chars, we use the trimmed option in proc sql
   to remove leading and trailing blanks in the caslib name.
   From macro provided by Wilbram Hazejager
*------------------------------------------------------------------------------------------*/

%macro _usr_getNameCaslib(_usr_LibrefUsingCasEngine); 

   %global _usr_nameCaslib;
   %let _usr_nameCaslib=;

   proc sql noprint;
      select sysvalue into :_usr_nameCaslib trimmed from dictionary.libnames
      where libname = upcase("&_usr_LibrefUsingCasEngine.") and upcase(sysname)="CASLIB";
   quit;

%mend _usr_getNameCaslib;

/* -------------------------------------------------------------------------------------------* 
    Macro to check whether Python is available to the compute session where this program runs.
    Identification done through the PROC_PYPATH environment variable. If Python is found, a macro 
    variable is created with the path.  If not, an error message is output and an error flag 
    specified by user is flagged with a 1 (to indicate an error).  Flag can be used in downstream
    code if specified as a global variable.
    Input:
    1. errorFlagName: Provide this with quotes when executing the macro. Name of an error flag macro 
                      variable. Specify this variable as global so that it can be used downstream.
    Output:
    1. PROC_PYPATH : A global variable which contains the path where Python can be found.
    2. Informational or error message as applicable written to log.
    3. errorFlagName macro variable modified if necessary.
    
    Also available at: https://raw.githubusercontent.com/SundareshSankaran/sas_utility_programs/main/code/Check_For_Python/macro_python_check.sas
 *-------------------------------------------------------------------------------------------- */

 %macro _env_check_python(errorFlagName);

     %global PROC_PYPATH;

     data _null_;
        proc_pypath = sysget('PROC_PYPATH');
        if proc_pypath = "" then do;
           call symputx(&errorFlagName.,1);
        end;
        else do;
           call symput("PROC_PYPATH", proc_pypath);
        end;
     run;

     %if "&PROC_PYPATH." = "" %then %do;
           %put ERROR: Python is not available or configured in this compute session ;
     %end;
     %else %do;
           %put NOTE: Python is available in this compute session;
     %end;

  %mend _env_check_python;


/*-----------------------------------------------------------------------------------------*
   EXECUTION CODE MACRO 
*------------------------------------------------------------------------------------------*/

%macro _hcdc_main_execution_code;

/*-----------------------------------------------------------------------------------------*
   Create an error flag. 
*------------------------------------------------------------------------------------------*/

   %_create_error_flag(_hcdc_error_flag);


/*-----------------------------------------------------------------------------------------*
   Check if Python's available in the environment. 
*------------------------------------------------------------------------------------------*/

   %_env_check_python("_hcdc_error_flag");

/*-----------------------------------------------------------------------------------------*
   Check if an active CAS session exists. 
*------------------------------------------------------------------------------------------*/

   %global casSessionExists;

   %if &_hcdc_error_flag.=0 %then %do;
      %_env_cas_checkSession("_hcdc_error_flag");
   %end;

/*-----------------------------------------------------------------------------------------*
   Check Input table libref to ensure it points to a valid caslib.
*------------------------------------------------------------------------------------------*/

   %if &_hcdc_error_flag. = 0 %then %do;

      %global inputCaslib;
   
      %_usr_getNameCaslib(&inputTable_lib.);
      %let inputCaslib=&_usr_nameCaslib.;
      %put NOTE: &inputCaslib. is the caslib for the input table.;
      %let _usr_nameCaslib=;

      %if "&inputCaslib." = "" %then %do;
         %put ERROR: Library selected for input table needs to point to a caslib. ;
         %let _hcdc_error_flag=1;
      %end;

   %end;

/*-----------------------------------------------------------------------------------------*
   Run Python block (accepts inputs and loads documents along with embeddings)
*------------------------------------------------------------------------------------------*/
   %if &_hcdc_error_flag. = 0 %then %do;

      proc python infile = hcdccode;
      quit;

      filename hcdccode clear;

   %end;

%mend _hcdc_main_execution_code;

/*-----------------------------------------------------------------------------------------*
   END MACRO DEFINITIONS
*------------------------------------------------------------------------------------------*/

/*-----------------------------------------------------------------------------------------*
   EXECUTION CODE
   The execution code is controlled by the trigger variable defined in this custom step. This
   trigger variable is in an "enabled" (value of 1) state by default, but in some cases, as 
   dictated by logic, could be set to a "disabled" (value of 0) state.
*------------------------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------------------*
   Create run-time trigger. 
*------------------------------------------------------------------------------------------*/

%_create_runtime_trigger(_hcdc_run_trigger);

%if &_hcdc_run_trigger. = 1 %then %do;

   %_hcdc_main_execution_code;

%end;
%if &_hcdc_run_trigger. = 0 %then %do;

   %put NOTE: This step has been disabled.  Nothing to do.;

%end;


/*-----------------------------------------------------------------------------------------*
   Clean up existing macro variables and macro definitions.
*------------------------------------------------------------------------------------------*/
%if %symexist(_hcdc_error_flag) %then %do;
   %symdel _hcdc_error_flag;
%end;

%if %symexist(casSessionExists) %then %do;
   %symdel casSessionExists;
%end;

%if %symexist(inputCaslib) %then %do;
   %symdel inputCaslib;
%end;

%if %symexist(PROC_PYPATH) %then %do;
   %symdel PROC_PYPATH;
%end;

%if %symexist(_current_uuid_) %then %do;
   %symdel _current_uuid_;
%end;

%if %symexist(_current_uuid_) %then %do;
   %symdel _current_uuid_;
%end;

%if %symexist(_hcdc_run_trigger) %then %do;
   %symdel _hcdc_run_trigger;
%end;


/*-----------------------------------------------------------------------------------------*
   Clean up existing macro variables and macro definitions.
*------------------------------------------------------------------------------------------*/
%sysmacdelete _hcdc_main_execution_code;
%sysmacdelete _env_check_python;
%sysmacdelete _usr_getNameCaslib;
%sysmacdelete _env_cas_checkSession;
%sysmacdelete _create_runtime_trigger;
%sysmacdelete _create_error_flag;

