# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger('root');

from validate_email import validate_email
import re
from itertools import cycle
from datetime import datetime
from pytz import timezone
import os
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine

#----------------------------------------------------------------------------
#PASO N° 20 - Transformar el archivo JSON en archivos CSV's. Uno por tabla.
#----------------------------------------------------------------------------

class check:
  def __init__(self, args):
    self.args = args;
    logger.info(f"tipo de argumento: {type(self.args)}, valores: {self.args}");
    self.functions = {
      "fn0FA": "self.fn0FA(conn)",
      "fn0FB": "self.fn0FB(conn)",
      "fn1FA": "No/Verificado",
      "fn1FB": "No/Verificado",
      "fn1FC": "No/Verificado",
      "fn2FA": "No/Verificado",
      "fn2EA": "No/Verificado",
      "fn2DA": "No/Verificado",
      "fn2DB": "No/Verificado",
      "fn2CA": "No/Verificado",
      "fn2CB": "No/Verificado",
      "fn2BA": "No/Verificado",
      "fn2AA": "self.fn2AA(conn)",
      "fn29A": "No/Verificado",
      "fn29B": "No/Verificado",
      "fn29C": "No/Verificado",
      "fn28A": "self.fn28A(conn)",
      "fn28B": "self.fn28B(conn)",
      "fn3F0": "self.fn3F0(conn)",
      "fn3F1": "self.fn3F1(conn)",
      "fn3F2": "self.fn3F2(conn)",
      "fn3F3": "self.fn3F3()",
      "fn3F4": "self.fn3F4()",
      "fn3F5": "self.fn3F5()",
      "fn3F6": "self.fn3F6()",
      "fn3F7": "self.fn3F7()",
      "fn3F8": "self.fn3F8()",
      "fn3F9": "self.fn3F9()",
      "fn3FA": "self.fn3FA()",
      "fn3FB": "self.fn3FB()",
      "fn3FC": "self.fn3FC()",
      "fn3FD": "self.fn3FD()",
      "fn3FE": "self.fn3FE(conn)",
      "fn3FF": "self.fn3FF()",
      "fn3E0": "self.fn3E0(conn)",
      "fn3E1": "self.fn3E1()",
      "fn3E2": "self.fn3E2(conn)",
      "fn3E3": "self.fn3E3()",
      "fn3E4": "self.fn3E4(conn)",
      "fn3E5": "self.fn3E5()",
      "fn3E6": "self.fn3E6()",
      "fn3E7": "self.fn3E7()",
      "fn3E8": "self.fn3E8()",
      "fn3E9": "self.fn3E9()",
      "fn3EA": "self.fn3EA()",
      "fn3EB": "self.fn3EB()",
      "fn3EC": "self.fn3EC()",
      "fn3ED": "self.fn3ED()",
      "fn3EE": "self.fn3EE()",
      "fn3EF": "self.fn3EF(conn)",
      "fn3D0": "No/Verificado",
      "fn3D1": "No/Verificado",
      "fn3D2": "No/Verificado",
      "fn3D3": "No/Verificado",
      "fn3D4": "No/Verificado",
      "fn3D5": "No/Verificado",
      "fn3D6": "No/Verificado",
      "fn3D7": "No/Verificado",
      "fn3D8": "No/Verificado",
      "fn4FA": "self.fn4fa(conn,idorga)",
      "fn5F0": "self.fn5f0(conn,fecha,organizacion)",
      "fn5E0": "No/Verificado",
      "fn5E1": "self.fn5E1(conn)",
      "fn5E2": "No/Verificado",
      "fn5E3": "No/Verificado",
      "fn5E4": "self.fn5E4(conn)",
      "fn5E5": "self.fn5E5(conn)",
      "fn5E6": "No/Verificado",
      "fn5D0": "self.fn5D0(conn date)",
      "fn6F0": "No/Verificado",
      "fn6F1": "No/Verificado",
      "fn6E0": "No/Verificado",
      "fn6E1": "No/Verificado",
      "fn6E2": "No/Verificado",
      "fn6E3": "No/Verificado",
      "fn6E4": "No/Verificado",
      "fn6E5": "No/Verificado",
      "fn6D0": "No/Verificado",
      "fn6D1": "No/Verificado",
      "fn6C0": "No/Verificado",
      "fn6C1": "No/Verificado",
      "fn6C2": "No/Verificado",
      "fn6B0": "No/Verificado",
      "fn6B1": "No/Verificado",
      "fn6A0": "No/Verificado",
      "fn6A1": "No/Verificado",
      "fn6A2": "No/Verificado",
      "fn6A3": "No/Verificado",
      "fn690": "No/Verificado",
      "fn680": "No/Verificado",
      "fn681": "No/Verificado",
      "fn682": "No/Verificado",
      "fn7F0": "No/Verificado",
      "fn7F1": "No/Verificado",
      "fn7F2": "No/Verificado",
      "fn7F3": "No/Verificado",
      "fn7F4": "No/Verificado",
      "fn7F5": "No/Verificado",
      "fn7F6": "No/Verificado",
      "fn8F0": "No/Verificado",
      "fn8F1": "self.fn8F1(conn)",
      "fn8F2": "self.fn8F2(conn)",
      "fn8F3": "self.fn8F3(conn)",
      "fn9F0": "No/Verificado",
      "fn9F1": "No/Verificado",
      "fn9F2": "self.fn9F2(conn)",
      "fn9F3": "self.fn9F3(conn)"
    }
    # self.dfLog = dfLog
    # self._encode = _encode
    # self._sep = _sep
    # self.secPhase = secPhase
    # self.path_to_DB_file = path_to_DB_file
    #t_stamp = datetime.timestamp(datetime.now(timezone('Chile/Continental')))
    self.args._FKErrorsFile = f'./{self.args.t_stamp}_ForenKeyErrors.csv';
    self.listValidations = self.cargarPlanillaConListasParaValidar();

  #----------------------------------------------------------------------------
  # Transforma archivo JSON en un DataFrame de pandas con todas sus columnas.
  # Agrega las columnas que faltan.
  #----------------------------------------------------------------------------
  def execute(self):
    _result = True
    engine = create_engine(f"sqlite+pysqlcipher://:{self.args.secPhase}@/{self.args.path_to_DB_file}?cipher=aes-256-cfb&kdf_iter=64000")
    try:
      conn = engine.connect()

      for key,value in self.functions.items():
        if(value != "No/Verificado"):
          logger.info(f"Ejecutando función {key} con los parámetros {value}")
          eval_ = eval(value)
          logger.info(f"Resultado de la evaluación de la función {key}: {eval_}")
          _result = eval_ and _result

      #_r[0] = self.verificaIntegridadReferencial(conn,self.args._FKErrorsFile)
      #self.verificaDatosDeLasPersonas(conn)
      #self.verificaDatosEstudiantes(conn)
      #self.verificaDatosDocentes(conn)
      #self.verificaDatosEstablecimiento(conn)
      #self.verificaJerarquiasOrganizacional(conn)
      #self.verificaClaveAleatoriaDocentes(conn)

      if(not _result):
        raise Exception("El archivo no cumple con el Estándar de Datos para la Educación. Hay errores en la revisión. Revise el LOG para más detalles")

    except Exception as e:
      _t = "ERROR en la validación: "+str(e)
      logger.info(_t);
      _result = False
    finally:
      #closind database connection
      conn.close()
    return True#_result

  #Carga planilla con todas las listas de validación desde Google Drive
  #https://drive.google.com/open?id=1vZD8ufVm3Z71V9TveQcLI0A02wrmwsz43z3TyWl9C-s
  def cargarPlanillaConListasParaValidar(self):
    #idFile = '1vZD8ufVm3Z71V9TveQcLI0A02wrmwsz43z3TyWl9C-s'
    #url = f'https://docs.google.com/spreadsheets/d/{idFile}/export?format=xlsx&id={idFile}'
    url = './ede/ede/listValidationData.xlsx';
    xd = pd.read_excel(url,'ListValidations')
    _t=f'Planilla {url} cargada satisfactoriamente'; logger.info(_t)
    return xd;

  #VERIFICA LA CONEXION A LA BASE DE DATOS
  def fn3F0(self,conn):
    _r = False;
    rows = conn.execute("SELECT * FROM PersonList;").fetchall()
    if(len(rows)>0): _r = True
    logger.info("Aprobado") if _r else logger.error("Rechazado");
    return _r

  #VERIFICA LA INTEGRIDAD REFERENCIAL DE LOS DATOS
  def fn3F1(self,conn):
    _r = True; _e = ''
    rows = conn.execute("PRAGMA foreign_key_check;")

    if(rows.returns_rows):
      pd.DataFrame(rows,columns=['Table', 'rowId', 'Parent', 'FkId']).to_csv(
          self.args._FKErrorsFile,sep=self.args._sep,encoding=self.args._encode,index=False)
      _e = f"BD con errores, más detallen en {self.args._FKErrorsFile}"
      _r = False

    logger.info(f"RESULTADO DE LA VERIFICACIÓN DE LA INTEGRIDAD REFERENCIAL: {_r}. {_e}");
    logger.error("Rechazado") if (_e != '') else logger.info("Aprobado")
    return _r

  #VERIFICA SI LA VISTA PersonList contiene información
  def fn3F2(self, conn):
    try:
      rows = conn.execute("""
        SELECT
          RUN
          ,IPE
          ,emailAddress
          ,emailAddressType
          ,TelephoneNumber
          ,telephoneNumberType
          ,numeroListaCurso
          ,numeroMatricula
          ,fechaIncorporacionEstudiante
          ,fechaRetiroEstudiante
          ,fechaCumpleanos
          ,TribalAffiliationDescription
        FROM PersonList where role like '%Estudiante%';
      """).fetchall()

      logger.info(f"len(personList): {len(rows)}")

      if(len(rows)>0):
        self.rutList = self.convertirArray2DToList(list([m[0] for m in rows if m[0] is not None])) # Valida lista de rut ingresados a la BD
        self.ipeList = self.convertirArray2DToList(list([m[1] for m in rows if m[1] is not None])) # Valida lista de IPE ingresados a la BD
        self.emailList = self.convertirArray2DToList(list([m[2] for m in rows if m[2] is not None])) #Valida que los email tengan el formato correcto
        self.emailTypeList = self.convertirArray2DToList(list([m[3] for m in rows if m[3] is not None])) #Valida que los emailType tengan el formato correcto
        self.phoneList = self.convertirArray2DToList(list([m[4] for m in rows if m[4] is not None])) # Valida que los teléfonos cumplan con el formato E164
        self.phoneTypeList = self.convertirArray2DToList(list([m[5] for m in rows if m[5] is not None])) # Valida que los tipos de teléfonos cumplan con el formato E164
        self.numListaList = self.convertirArray2DToList(list([m[6] for m in rows if m[6] is not None])) # Valida los número de lista de los cursos sean número
        self.numMatriculaList = self.convertirArray2DToList(list([m[7] for m in rows if m[7] is not None])) # Valida los números de matrícula del establecimiento sean número
        self.fechaIncorporacionEstudianteList = self.convertirArray2DToList(list([m[8] for m in rows if m[8] is not None])) # Valida que las fechas cumplan con el formato de fecha
        self.fechaRetiroEstudianteList = self.convertirArray2DToList(list([m[9] for m in rows if m[9] is not None])) # Valida que las fechas cumplan con el formato de fecha
        self.fechaCumpleanosList = self.convertirArray2DToList(list([m[10] for m in rows if m[10] is not None])) # Valida que las fechas cumplan con el formato de fecha
        self.AllDatesList = self.fechaIncorporacionEstudianteList+self.fechaRetiroEstudianteList+self.fechaCumpleanosList # Valida que las fechas cumplan con el formato de fecha
        self.TribalList = self.convertirArray2DToList(list(set([m[11] for m in rows if m[11] is not None]))) # Valida que las afiliaciones tribales sean las permitidas en Chile
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE RUN INGRESADAS EN EL SISTEMA ES VALIDA
  def fn3F3(self):
    try:
      _l = self.rutList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validarRUN(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL RUN DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE IPE INGRESADOS EN EL SISTEMA ES VALIDA
  def fn3F4(self):
    try:
      _l = self.ipeList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validarIpe(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL IPE DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE e-mails INGRESADOS EN EL SISTEMA CUMPLE CON EL FORMATO
  def fn3F5(self):
    try:
      _l = self.emailList
      if(len(_l)>0):
        _err = set([e for e in _l if not validate_email(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DE FORMATO DE LOS E-MAILS DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE teléfonos INGRESADOS cumple con el formato E164
  def fn3F6(self):
    try:
      _l = self.phoneList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoE164Telefono(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DE LOS TELEFONOS DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI EL NUMERO DE LISTA cumple con el formato
  def fn3F7(self):
    try:
      _l = self.numListaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoNumero(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DEL NUMERO DE LISTA DE LAS PERSONAS: {_r}. {_err}"
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI EL NUMERO DE MATRICULA cumple con el formato
  def fn3F8(self):
    try:
      _l = self.numMatriculaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoNumero(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL NUMERO DE MATRICULA DE LAS PERSONAS: {_r}. {_err}"
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE FECHAS INGRESADAS cumple con el formato
  def fn3F9(self):
    try:
      _l = self.AllDatesList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoFecha(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DE LAS DE LAS PERSONAS: {_r}. {_err}"
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE afiliaciones tribales se encuentra dentro de la lista permitida
  def fn3FA(self):
    try:
      _l = self.TribalList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaTribalAffiliation(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DE LA LISTA DE AFILIACIONES TRIBALES DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #Valida que la cantidad de #matricula == #Lista == #fechasIncorporaciónes
  def fn3FB(self):
    try:
      _l1 = self.numListaList;_l2 = self.numMatriculaList;_l3 = self.fechaIncorporacionEstudianteList;
      if(len(_l1)>0 and len(_l2)>0 and len(_l3)>0):
        _r   = len(_l1) == len(_l2) == len(_l3)
        _t = f"Valida que la cantidad de #matricula == #Lista == #fechasIncorporaciónes: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA que la cantidad de emails corresponda con los tipos de emails ingresados
  def fn3FC(self):
    try:
      _l1 = self.emailList;_l2 = self.emailTypeList
      if(len(_l1)>0 and len(_l2)>0):
        _r   = len(_l1) == len(_l2)
        _t = f"VERIFICA que la cantidad de e-mails corresponda con los tipos de e-mails ingresados en las personas: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA que la cantidad de teléfonos corresponda con los tipos de teléfonos ingresados
  def fn3FD(self):
    try:
      _l1 = self.phoneList;_l2 = self.phoneTypeList
      if(len(_l1)>0 and len(_l2)>0):
        _r   = len(_l1) == len(_l2)
        _t = f"VERIFICA que la cantidad de teléfonos corresponda con los tipos de teléfonos ingresados en las personas: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA VISTA PersonList filtrada por estudiantes contiene información
  def fn3FE(self, conn):
    try:
      rows = conn.execute("""
      SELECT
        personId
        ,ciudadNacimiento
        ,regionNacimiento
        ,paisNacimiento
      FROM PersonList
      WHERE Role like '%Estudiante%';
    """).fetchall()

      logger.info(f"len(estudiantes): {len(rows)}")

      if(len(rows)>0):
        personId  = self.convertirArray2DToList(list([m[0] for m in rows if m[0] is not None]))
        cuidadNac = self.convertirArray2DToList(list([m[1] for m in rows if m[1] is not None]))
        regionNac = self.convertirArray2DToList(list([m[2] for m in rows if m[2] is not None]))
        paisNac   = self.convertirArray2DToList(list([m[3] for m in rows if m[3] is not None]))
        self.comparaEstudiantes = [len(personId) == len(cuidadNac) == len(regionNac) == len(paisNac)]
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList filtrada por estudiantes: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE TODOS LOS ESTUDIANTES TENGAN Pais, Región y cuidad de nacimiento
  def fn3FF(self):
    try:
      _l = self.comparaEstudiantes
      if(len(_l)>0):
        _r   = _l
        _t = f"VERIFICA QUE TODOS LOS ESTUDIANTES TENGAN Pais, Región y cuidad de nacimiento: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA VISTA PersonList filtrada por docentes contiene información
  def fn3E0(self, conn):
    try:
      rows = conn.execute("""
      SELECT
        personId
        ,DegreeOrCertificateTitleOrSubject
        ,DegreeOrCertificateTypeDescription
        ,AwardDate
        ,NameOfInstitution
        ,higherEducationInstitutionAccreditationStatusDescription
        ,educationVerificationMethodDescription
      FROM PersonList
      WHERE Role like '%Docente%';
    """).fetchall()

      logger.info(f"len(docentes): {len(rows)}")

      if(len(rows)>0):
        personId            = self.convertirArray2DToList(list([m[0] for m in rows if m[0] is not None]))
        title               = self.convertirArray2DToList(list([m[1] for m in rows if m[1] is not None]))
        Type                = self.convertirArray2DToList(list([m[2] for m in rows if m[2] is not None]))
        AwardDate           = self.convertirArray2DToList(list([m[3] for m in rows if m[3] is not None]))
        Institution         = self.convertirArray2DToList(list([m[4] for m in rows if m[4] is not None]))
        AccreditationStatus = self.convertirArray2DToList(list([m[5] for m in rows if m[5] is not None]))
        VerificationMethod  = self.convertirArray2DToList(list([m[6] for m in rows if m[6] is not None]))
        self.comparaDocentes = [len(personId) == len(title) == len(Type) == len(AwardDate) == len(Institution) == len(AccreditationStatus) == len(VerificationMethod)]
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList filtrada por docentes: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE TODOS LOS DOCENTES TENGAN su título y la institución de educación ingresados en el sistema
  def fn3E1(self):
    try:
      _l = self.comparaDocentes
      if(len(_l)>0):
        _r   = _l
        _t = f"VERIFICA QUE TODOS LOS DOCENTES TENGAN su título y la institución de educación ingresados en el sistema: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA TABLA k12schoolList unida a organizationList contiene información
  def fn3E2(self, conn):
    try:
      rows = conn.execute("SELECT Identifier FROM k12schoolList INNER JOIN organizationList USING(OrganizationId);").fetchall()
      logger.info(f"len(establecimientos): {len(rows)}")
      if(len(rows)>0):
        self.formatoRBD = self.convertirArray2DToList(list(set([m[0] for m in rows if m[0] is not None])))
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la tabla k12schoolList para identificar el RBD del establecimiento: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL FORMATO DEL RBD CORRESPONDA
  def fn3E3(self):
    try:
      _l = self.formatoRBD
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoRBD(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DEL RBD DEL ESTABLECIMIENTO: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA VISTA jerarquiasList contiene información
  def fn3E4(self, conn):
    try:
      rows = conn.execute("SELECT RBD,nombreEstablecimiento,modalidad,jornada,nivel,rama,sector,especialidad,tipoCurso,codigoEnseñanza,grado,letraCurso  FROM jerarquiasList;").fetchall()
      logger.info(f"len(organizaciones): {len(rows)}")
      if(len(rows)>0):
        self.modalidadList = self.convertirArray2DToList(list(set([m[2] for m in rows if m[2] is not None])))
        self.jornadaList = self.convertirArray2DToList(list(set([m[3] for m in rows if m[3] is not None])))
        self.nivelList = self.convertirArray2DToList(list(set([m[4] for m in rows if m[4] is not None])))
        self.ramaList = self.convertirArray2DToList(list(set([m[5] for m in rows if m[5] is not None])))
        self.sectorList = self.convertirArray2DToList(list(set([m[6] for m in rows if m[6] is not None])))
        self.especialidadList = self.convertirArray2DToList(list(set([m[7] for m in rows if m[7] is not None])))
        self.tipoCursoList = self.convertirArray2DToList(list(set([m[8] for m in rows if m[8] is not None])))
        self.codigoEnseList = self.convertirArray2DToList(list(set([m[9] for m in rows if m[9] is not None])))
        self.gradoList = self.convertirArray2DToList(list(set([m[10] for m in rows if m[10] is not None])))
        self.letraCursoList = self.convertirArray2DToList(list(set([m[11] for m in rows if m[11] is not None])))
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista jerarquiasList para obtener la lista de organizaciones: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA MODALIDAD ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3E5(self):
    try:
      _l = self.modalidadList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaModalidad(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA MODALIDAD ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA JORNADA ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3E6(self):
    try:
      _l = self.jornadaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaJornada(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA JORNADA ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL NIVEL ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3E7(self):
    try:
      _l = self.nivelList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaNivel(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL NIVEL ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA RAMA ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3E8(self):
    try:
      _l = self.ramaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaRama(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA RAMA ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL SECTOR ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3E9(self):
    try:
      _l = self.sectorList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaSector(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL SECTOR ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA ESPECIALIDAD ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3EA(self):
    try:
      _l = self.especialidadList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaEspecialidad(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA ESPECIALIDAD ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL TIPO DE CURSO ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3EB(self):
    try:
      _l = self.tipoCursoList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaTipoCurso(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL TIPO DE CURSO ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL CODIGO DE ENSEÑANZA ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3EC(self):
    try:
      _l = self.codigoEnseList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaCodigoEnse(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL CODIGO DE ENSEÑANZA ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL GRADO ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3ED(self):
    try:
      _l = self.gradoList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaGrado(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL GRADO ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA LETRA DEL CURSO ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3EE(self):
    try:
      _l = self.letraCursoList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaLetraCurso(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA LETRA DEL CURSO ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE COINCIDAN LOS ID'S DE LOS CURSOS EN LAS DIFERENTES TABLAS
  def fn3EF(self, conn):
    try:
      courses1 = conn.execute("SELECT OrganizationIdDelCurso FROM jerarquiasList;").fetchall()
      courses2 = conn.execute("SELECT OrganizationIdCurso FROM cursoList;").fetchall()
      logger.info(f"Vista jerarquiasList: {len(courses1)}, Vista cursoList: {len(courses2)}")
      if(len(courses1)>0 and len(courses2)>0):
        # Valida que lista de cursos coincidan
        curso1 = list(set([m[0] for m in courses1 if m[0] is not None]))
        curso2 = list(set([m[0] for m in courses2 if m[0] is not None]))
        _c = len(set(curso1) & set(curso2))
        _err = "No coinciden los ID de Curso en las tablas Organization + Course + K12Course"
        logger.info(f"Aprobado") if _c == len(curso1) == len(curso2) else logger.error(_err)
      else:
        logger.error(f"S/Datos")

      return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista jerarquiasList para obtener la lista de organizaciones: {str(e)}")
      logger.error(f"Rechazado")
      return False

  def separaRUT(self, *args):
    if (len(args) > 0) and (args[0] is not None):
      dv = ''.join([c for c in list(args[0].upper()) if c.isalpha()])
      aux = ''.join([c for c in list(args[0]) if c.isdigit()])
      if(dv == ''):
        dv = aux[-1:]
        aux = aux[:-1]
    else:
      aux=0
      dv=0
    return aux,dv

  def validarRut(self, aux,dv):
    revertido = map(int, reversed(str(aux)))
    factors = cycle(range(2,8))
    s = sum(d * f for d, f in zip(revertido,factors))
    res = (-s)%11
    if ((str(res) == dv) or (dv=="K" and res==10)):
      return True
    return False

  #VERIFICA SI EL RUT INGRESADO ES VALIDO
  def validarRUN(self, *args):
    aux,dv = self.separaRUT(*args)
    if(aux!=0 and dv!=0):
      if(self.validarRut(aux,dv)):
        if(int(aux)<=47000000):
          return True
    return False

  def validarIpe(self, *args):
    aux,dv = self.separaRUT(*args)
    if(aux!=0 and dv!=0):
      if(self.validarRut(aux,dv)):
        if(int(aux)>=100000000):
          return True
    return False

  def convertirArray2DToList(self, text):
    _l = []
    for e in text:
      if "|" not in str(e):
        _l.append(e)
      else:
        for subE in e.split("|"):
          _l.append(subE)
    return _l

  def imprimeErrores(self, lista,fn,msg):
    _l = self.convertirArray2DToList(lista)
    _err = set([e for e in _l if not fn(e)])
    _r   = False if len(_err)>0 else True
    _t = f"{msg}: {_r}. {_err}";logger.info(_t);
    return _err,_r

  def validaFormatoE164Telefono(self, e):
    r = re.compile('^\+56\d{9,15}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaFormatoNumero(self, e):
    r = re.compile('^\d{0,4}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaFormatoFecha(self, e):
    r = re.compile('^((19|20)(\d{2})-(1[0-2]|0?[0-9])-([12][0-9]|3[01]|0?[1-9]))[ T]?((0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.\d{0,})?)?([+-](0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))?$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaTribalAffiliation(self, e):
    _lista = list(set(self.listValidations['TribalAffiliationDescriptionList']))
    return True if e in _lista else False

  def validaBoolean(self, e):
    return e

  # VERIFICA DATOS DE LAS ORGANIZACIONES
  # VERIFICA JERARQUIA DE LOS DATOS
  # la jerarquí es:
  #  RBD -> Modalidad -> Jornada -> Niveles -> Rama ->
  #  Sector Económico -> Especialidad ->
  #  Tipo de Curso -> COD_ENSE -> Grado -> Curso -> Asignatura
  def validaFormatoRBD(self, e):
    r = re.compile('^RBD\d{5}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaModalidad(self, e):
    _lista = list(set(self.listValidations['modalidadesList']))
    return True if e in _lista else False

  def validaJornada(self, e):
    _lista =  list(set(self.listValidations['jornadasList']))
    return True if e in _lista else False

  def validaNivel(self, e):
    _lista =  list(set(self.listValidations['nivelList']))
    return True if e in _lista else False

  def validaRama(self, e):
    _lista =  list(set(self.listValidations['ramaList']))
    return True if e in _lista else False

  def validaSector(self, e):
    _lista =  list(set(self.listValidations['sectorList']))
    return True if e in _lista else False

  def validaEspecialidad(self, e):
    _lista =  list(set(self.listValidations['especialidadList']))
    return True if e in _lista else False

  def validaTipoCurso(self, e):
    _lista =  list(set(self.listValidations['tipoCursoList']))
    return True if e in _lista else False

  def validaCodigoEnse(self, e):
    _lista =  list(set(self.listValidations['codigoEnseList']))
    return True if e in _lista else False

  def validaGrado(self, e):
    _lista =  list(set(self.listValidations['gradoList']))
    return True if e in _lista else False

  def validaLetraCurso(self, e):
    r = re.compile('^[A-Z]{1,2}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaFormatoClaveAleatoria(self, e):
    r = re.compile('^[0-9]{6}+([-]{1}[0-9kK]{1})?$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  # VERIFICA DATOS DE LAS ORGANIZACIONES
  def verificaClaveAleatoriaDocentes(self, conn):
    _r = False;error=''
    rows = conn.execute("SELECT ClaveAleatoriaDocente FROM oprList where ClaveAleatoriaDocente not null;").fetchall()
    logger.info(f"len(ClaveAleatoriaDocente): {len(rows)}")
    if(len(rows)>0):
      # Valida los números de clave aleatoria de los docentes
      data = list(set([m[0] for m in rows if m[0] is not None]))
      _err,_r = self.imprimeErrores(data,self.validaFormatoClaveAleatoria,"VERIFICA FORMATO Clave Aleatoria Docente")
    else:
      _err = "La BD no contiene clave aleatoria de los docentes"

    _t = f"VERIFICA CLAVE ALEATORIA DOCENTES: {_r}"
    logger.info(_t);
    return _r

### Appoderado INICIO ###
### Appoderado FIN ###

## WebClass INICIO ##

## Inicio fn8F1 WC ##
  def fn8F1(self, conn):
          try:
              query = conn.execute("""select * from Incident""").fetchall()
              if(len(query)>0):
                  Incidentes = (list([m[0] for m in query if m[0] is not None]))
                  for x in Incidentes:
                      querySelect = "select * from K12StudentDiscipline where IncidentId = "
                      queryWhere = str(x)
                      queryComplete = querySelect+queryWhere
                      try:
                          query = conn.execute(queryComplete).fetchall()
                          if(len(query)>0):
                              query = len(query)
                              logger.info(f'Total de datos: {query}')
                              logger.info(f'Aprobado')
                              return True
                          else:
                              logger.error(f'S/Datos')
                              logger.error(f'Rechazado')
                              return False
                      except Exception as e:
                          logger.error(f'No se pudo ejecutar la consulta: {str(e)}')
                          logger.error(f'Rechazado')
                          return False
              else:
                  logger.error(f'S/Datos')
                  logger.error(f'Rechazado')
                  return True
          except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn8F1 WC ##

## Inicio fn5E4 WC ##
  def fn5E4(self,conn):
      try:
          i=0
          asistencia_alumnos=conn.execute("""select P.FirstName,P.LastName,o.OrganizationId,RAE.VirtualIndicator,CSL.OrganizationId,RAE.Date,Cr.ClassroomIdentifier,RAE.RefAttendanceStatusId,RAE.RefAbsentAttendanceCategoryId,RAE.RefLeaveEventTypeId,RAE.RefPresentAttendanceCategoryId,RAE.observaciones from Organization o
          join OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
          join RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
          join Person P on OPR.PersonId = P.PersonId
          join Location L on  L.LocationId = o.OrganizationId
          join Classroom Cr on L.LocationId = Cr.LocationId
          join CourseSectionLocation CSL on CR.LocationId = CSL.LocationId
          join CourseSection CS on CSL.OrganizationId = CS.OrganizationId
      where OPR.RoleId = 6 ; """).fetchall()
          a=len(asistencia_alumnos)
          if i<=a:
              for fila in asistencia_alumnos:
                  if fila[8] is None:
                      logger.error(f"No esta registrado el estatus de la asistencia")
                      logger.error(f"Rechazado")
                      return False
                  else:
                      if fila[3] is not None:
                          if fila[3]==1:
                              logger.info(f"asistencia Virtual")
                          elif fila[3]==0:
                              logger.info(f"asistencia presencial")
                      else:
                          logger.error(f"el campo virtualindicator esta vacio")
                          logger.error(f"Rechazado")
                          return False
                      if fila[7]==1:continue
                      else:
                          if fila[7] ==  2 or 3:
                              if fila[8] is not None:
                                  continue
                              else:
                                  logger.error(f" descripcion de la ausencia sin datos en la asistencia")
                                  logger.error(f"Rechazado")
                                  return False
                          elif fila[7] == 4 or 6:
                              if fila[10] is not None:continue
                              else:
                                  logger.error(f"descripcion del status sin datos en la asistencia")
                                  logger.error(f"Rechazado")
                                  return False
                          elif fila[7] == 5:
                              if fila[9] is not None:continue
                              else:
                                  logger.error(f"descripcion de la salida temprana sin datos ")
                                  logger.error(f"Rechazado")
                                  return False
                  if i==a:
                      logger.info(f'Verificacion Terminada, todos los alumnos cuentan con registro de asistencia y su estatus')
                      logger.info(f'Validacion Aprobada')
                      return True
                  else: i+=1
          else:
              logger.error(f"S/datos")
              logger.error(f"Rechazado")
              return False
      except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn5E4 WC ##
## Inicio fn5E5 WC ##
  def fn5E5(self,conn):
        try:
            _query = conn.execute("""
            select
                RAE.digitalRandomKey,
                RAE.Date
            from Organization O
                    join OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
                    join RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
            where O.RefOrganizationTypeId = 22
            and OPR.RoleId in (4, 5)
            and RAE.Date between(RAE.Date||'10:00:00') and (RAE.Date||'11:45:00');
            """).fetchall()
            _lenQuery = int(len(_query))
            _contador = 0
            if(len(_query)>0):
                _digitalRandomKey = (list([m[0] for m in _query if m[0] is not None]))
                if not _digitalRandomKey:
                    logger.error(f"Sin firmas registradas")
                    logger.error(f'Rechazado')
                    return False
                for x in _digitalRandomKey:
                    if x is None:
                        logger.error(f'Sin firmas registradas')
                        logger.error(f'Rechazado')
                        return False
                    if (len(x)>0):
                        _contador += 1
                if(_lenQuery == _contador):
                    logger.info(f'Firmas registradas correctamente')
                    logger.info(f'Aprobado')
                    return True
            else:
                logger.error(f'S/Datos')
                logger.error(f'Rechazado')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn5E5 WC ##

## Inicio fn5D0 WC ##
  def fn5D0(self, conn):
        try:
            _oPR = conn.execute("""
                SELECT DISTINCT count(RAE.Date), OPR.PersonId, RAE.Date, RAE.digitalRandomKey,RAE.VirtualIndicator
                FROM OrganizationPersonRole OPR
                        JOIN RoleAttendanceEvent RAE ON OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
                WHERE OPR.RoleId in(4,5)
                AND RAE.RefAttendanceEventTypeId = 2
                group by OPR.PersonId, RAE.Date, RAE.digitalRandomKey, RAE.VirtualIndicator;
                """
                ).fetchall()
            if(len(_oPR)>0):
                _count = (list([m[0] for m in _oPR if m[0] is not None]))
                _contador = 0
                for x in _count:
                    if(x > 1):
                        _contador += 1
                if(_contador > 0):
                    logger.error('Duplicados')
                    logger.error('Rechazado')
                    return False
                else:
                    logger.info('No hay duplicados')
                    logger.info('Aprobado')
                    return True
            else:
                logger.error(f'No existen materias')
                logger.error(f'Rechazado')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## fin fn5D0 WC ##

## Inicio fn4FA WC ##
  def fn4FA(self, conn):
            try:
            #trae todos los datos bases del alumno y de su apoderado por curso
                idorga = conn.execute("""
                SELECT
                    organizationId,
                    name
                from Organization
                where RefOrganizationTypeId = 21
                """).fetchall()
                x=0
                for org in idorga:
                    print(org)
                    listaAlumno = conn.execute("""
                    SELECT op.personid,
                        op.organizationpersonroleid,
                        StudentListNumber                                                                        as "numero de lista",
                        Identifier                                                                               as rut,
                        p.Birthdate                                                                              as "fecha de nacimiento",
                        k12SE.FirstEntryDateIntoUSSchool,
                        rf.Definition                                                                            as sexo,
                        pad.StreetNumberAndName                                                                  as direccion
                            ,
                        (p.FirstName || ' ' || p.MiddleName || ' ' || p.LastName || ' ' || p.SecondLastName)     as "nombre completo"
                            ,
                        (p2.FirstName || ' ' || p2.MiddleName || ' ' || p2.LastName || ' ' || p2.SecondLastName) as "apoderado",
                        pad2.StreetNumberAndName                                                                 as "domicilio Apoderado"
                            ,
                        pt2.TelephoneNumber                                                                      as "fono apoderado",
                        pea2.EmailAddress                                                                        as "email apoderado"
                            ,
                        k12SE.FirstEntryDateIntoUSSchool                                                         as "fecha de inicio"
                        from K12StudentEnrollment k12SE
                            join OrganizationPersonRole op on (k12SE.OrganizationPersonRoleId = op.OrganizationPersonRoleId)
                            join PersonIdentifier PI on op.PersonId = PI.PersonId
                            join Person p on op.PersonId = p.PersonId
                            join refsex rf on p.RefSexId = rf.RefSexId
                            left join PersonAddress pad on p.PersonId = pad.PersonId
                            left join PersonRelationship pr on p.PersonId = pr.RelatedPersonId
                            left join Person p2 on p2.PersonId = pr.PersonId
                            left join PersonAddress pad2 on pad2.PersonId = p2.PersonId
                            left join PersonTelephone pt2 on pt2.PersonId = p2.PersonId
                            left join PersonEmailAddress pea2 on pea2.PersonId = p2.PersonId
                    where op.RoleId = 6
                    and length(Identifier) > 5
                    and op.organizationid = ?
                    group by op.PersonId;
                    """,([org[0]])).fetchall()
                    if(len(listaAlumno)> 0):
                        x=x+1
                        for alumno in listaAlumno:
                            #por cada alumno trae a los profesorees que interactuan con el
                            idAlumno=alumno[0]
                            idAlumnorole=alumno[1]
                            listaProfesionales = conn.execute("""
                            select (p.FirstName || ' ' || p.MiddleName || ' ' || p.LastName || ' ' || p.SecondLastName) as "nombre completo"
                            from OrganizationPersonRole op
                                    join OrganizationPersonRole op2 on op.OrganizationId = op2.OrganizationId
                                    join Person p on op.PersonId = p.PersonId
                            where op.roleid != 6
                            and op2.PersonId = ?
                            and op2.OrganizationID = ?
                            group by op.PersonId
                            """,(idAlumno,org[0])).fetchall()
                            #lista de becas e identificacion de estudiante preferente, prioritario, etc de ser requerido
                            listaPrograma = conn.execute("""
                            select rpt.description
                            from refparticipationtype rpt
                                    join PersonProgramParticipation ppp on rpt.RefParticipationTypeId = ppp.RefParticipationTypeId
                            where ppp.OrganizationPersonRoleId = ?;
                            """,([idAlumnorole])).fetchall()
                            #trae las asignaturas en las que se encuentra el alumno
                            organizacion = conn.execute("""
                            select op.OrganizationId, personid
                            from OrganizationPersonRole op
                                    join Organization o on op.OrganizationId = o.OrganizationId
                            where personid in (?);""",([idAlumno])).fetchall()
                            organi=[]
                            evalua=[]
                            for org in organizacion:
                                #por cada asignatura trae el calendario
                                calendario = conn.execute("""
                                select
                                    BeginDate,
                                    EndDate,
                                    SessionStartTime,
                                    SessionEndTime
                                from calendarList
                                where OrganizationId = ?
                                and "RefSessionType.Description" like '%Semester%';
                                    """,([org[0]])).fetchall()
                                if(calendario):
                                    organi.append(calendario)
                                # por cada asignatura trae las evaluaciones
                                evaluaciones = conn.execute(""" select name,begindate,CalendarDescription from calendarList where OrganizationId=? and CalendarDescription like '%Evaluacion%';""",([org[0]])).fetchall()
                                if (evaluaciones):
                                    evalua.append(evaluaciones)
                        becasprogramas=(list([m[0] for m in listaPrograma if m[0] is not None]))
                        evalua=(list([m[0] for m in evaluaciones if m[0] is not None]))
                        profe=(list([m[0] for m in listaProfesionales if m[0] is not None]))
                        calenda=(list([m[0] for m in calendario if m[0] is not None]))
                        print(profe)
                        if not profe:
                            logger.error(f"Sin profesores")
                            logger.error(f"Rechazado")
                            return False
                        elif not evalua:
                            logger.error(f"Sin evaluaciones")
                            logger.error(f"Rechazado")
                            return False
                        elif not calenda:
                            logger.error(f"Sin calendario")
                            logger.error(f"Rechazado")
                            return False
                    else:
                        logger.error(f"Sin Datos de alumnos: {org}")
                if x==0:
                  logger.error(f"Sin Datos de alumnos")
                  logger.error(f"Rechazado")
                  return False
                logger.info("Se validaron todos los datos")
                logger.info(f"Aprobado")
                return True
            except Exception as e:
                logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                logger.error(f"Rechazado")
                return False
## Fin fn4FA WC ##
## Inicio fn5F0 WC ##
  def fn5F0(self, conn):
        try:
            idorga= conn.execute("""
                select organizationId from organization where RefOrganizationTypeId = 22
            """).fetchall()
            for org in idorga:
                alumnosPresentes = conn.execute("""select count(rae.date),opr.organizationid,  strftime('%d', `Date`) as 'dia',strftime('%m', `Date`) as 'mes', date as hora from RoleAttendanceEvent rae
                join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                where refattendancestatusid = 1  and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
                group by  rae.date , opr.OrganizationId order  by rae.date;""",([org[0]])).fetchall()
                alumnosAusentes = conn.execute("""select count(rae.date),opr.organizationid from RoleAttendanceEvent rae
                join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                where refattendancestatusid in (2,3)  and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
                group by  rae.date , opr.OrganizationId order  by rae.date;""",([org[0]])).fetchall()
                alumnosRetrasados = conn.execute("""select count(rae.date) from RoleAttendanceEvent rae
                join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                where refattendancestatusid = 4  and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
                group by  rae.date , opr.OrganizationId order  by rae.date;""",([org[0]])).fetchall()
                profesorObservacion = conn.execute("""select Identifier,observaciones,rae.OrganizationPersonRoleId,opr.OrganizationId from PersonIdentifier PI
                join OrganizationPersonRole OPR on PI.PersonId=OPR.PersonId
                join RoleAttendanceEvent RAE on Rae.OrganizationPersonRoleId=opr.OrganizationPersonRoleId
                where  opr.RoleId !=6  and opr.organizationid = ?;""",([org[0]])).fetchall()
                if (alumnosPresentes):
                    fecha2=(list([m[4] for m in alumnosPresentes if m[4] is not None]))
                    asignatura = conn.execute("""select ClassPeriod,name from CourseSectionSchedule css join Organization o on o.OrganizationId=css.OrganizationId
                      where  ? between ClassBeginningTime and ClassEndingTime;""",(fecha2)).fetchall()
                    aPresentes=(list([m[0] for m in alumnosPresentes if m[0] is not None]))
                    seccion=(list([m[1] for m in alumnosPresentes if m[1] is not None]))
                    dia=(list([m[2] for m in alumnosPresentes if m[2] is not None]))
                    mes=(list([m[3] for m in alumnosPresentes if m[3] is not None]))
                    periodo=(list([m[0] for m in asignatura if m[0] is not None]))
                    name=(list([m[1] for m in asignatura if m[1] is not None]))
                    if not alumnosAusentes :
                        aAusentes=0
                    else:
                        aAusentes=alumnosAusentes[0][0]


                    if not alumnosRetrasados :
                        aRetrasados=0
                    else:
                        aRetrasados=alumnosRetrasados[0]


                    if not profesorObservacion :
                        logger.error(f"Sin profesores")
                        logger.error(f"Rechazado")
                        return False
                    if not dia:
                        logger.error(f"Sin dia")
                        logger.error(f"Rechazado")
                        return False
                    if not mes:
                        logger.error(f"Sin mes")
                        logger.error(f"Rechazado")
                        return False
                    if not seccion:
                        logger.error(f"Sin seccion")
                        logger.error(f"Rechazado")
                        return False
                    if not aPresentes:
                        logger.error(f"Sin presentes")
                        logger.error(f"Rechazado")
                        return False
                    if not name:
                        logger.error(f"Sin asignatura")
                        logger.error(f"Rechazado")
                        return False
                    if not periodo:
                        logger.error(f"Sin periodo")
                        logger.error(f"Rechazado")
                        return False
                else:
                    logger.error(f"S/Datos")
                    return True
            logger.info(f'Validacion aprobada')
            logger.info(f'Aprobado')
            return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
  ## Fin fn5F0 WC ##

## Inicio fn8F3 WC ##
  def fn8F3(self, conn):
        try:
          queryTwo = conn.execute('select * from k12StudentDiscipline where refDisciplinaryActionTakenId = 8').fetchall()
          if(len(queryTwo)>0):
              for fila in queryTwo:
                  for dato in fila:
                      if dato:
                          a = 0
                      else:
                          logger.error(f'Datos incompletos')
                          logger.error(f'Rechazado')
                          return False
          else:
              logger.error(f'S/Datos')
              return True
          logger.info(f'Datos Validados')
          logger.info(f'Aprobado')
          return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn8F3 WC ##

## Inicio fn8F2 WC ##
  def fn8F2(self,conn):
        try:
            _queryIncident = conn.execute("""
            select DISTINCT
                I.IncidentId,
                I.IncidentDate,
                I.IncidentTime,
                I.RefIncidentTimeDescriptionCodeId,
                I.IncidentDescription,
                I.OrganizationPersonRoleId,
                OPR.RoleId
                from Incident I
                join OrganizationPersonRole OPR on I.OrganizationPersonRoleId = OPR.OrganizationPersonRoleId;
                """).fetchall()
            _lenQuery = len(_queryIncident)
            if(len(_queryIncident)>0):
                _incidentId = (list([m[0] for m in _queryIncident if m[0] is not None]))
                if not _incidentId:
                    logger.error(f"Sin Incidentes")
                    logger.error(f'Rechazado')
                    return False
                _incidentDate = (list([m[1] for m in _queryIncident if m[1] is not None]))
                if not _incidentDate:
                    logger.error(f"Sin Fecha registrada para los incidentes")
                    logger.error(f'Rechazado')
                    return False
                _incidentTime = (list([m[2] for m in _queryIncident if m[2] is not None]))
                if not _incidentTime:
                    logger.error(f"Sin Time registrada para los incidentes")
                    logger.error(f'Rechazado')
                    return False
                _incidentTimeDescriptionCodeId = (list([m[3] for m in _queryIncident if m[3] is not None]))
                if not _incidentTimeDescriptionCodeId:
                    logger.error(f"Sin time code en el incidente")
                    logger.error(f'Rechazado')
                    return False
                _incidentDescription = (list([m[4] for m in _queryIncident if m[4] is not None]))
                if not _incidentDescription:
                    logger.error(f"Sin descripcion para el incidente")
                    logger.error(f'Rechazado')
                    return False
                _organizationPersonRoleId = (list([m[5] for m in _queryIncident if m[5] is not None]))
                if not _organizationPersonRoleId:
                    logger.error(f"Sin estudiante en incidente")
                    logger.error(f'Rechazado')
                    return False
                _roleId = (list([m[6] for m in _queryIncident if m[6] is not None]))
                if _roleId != 6:
                    logger.error(f"Sin estudiante en incidente")
                    logger.error(f'Rechazado')
                    return False
                for x in _incidentId:
                    _queryIncidentPerson = conn.execute("""
                    select DISTINCT
                    I.IncidentId,
                    I.PersonId,
                    I.RefIncidentPersonRoleTypeId,
                    OPR.RoleId
                    from IncidentPerson I
                            join Person P on I.PersonId = P.PersonId
                            join OrganizationPersonRole OPR on P.PersonId = OPR.PersonId
                    where I.IncidentId =
                    """,int(x)).fetchall()
                    if (len(_queryIncidentPerson)>0):
                        _personId = (list([m[1] for m in _queryIncidentPerson if m[1] is not None]))
                        _refRoleType = (list([m[2] for m in _queryIncidentPerson if m[2] is not None]))
                        _role = (list([m[3] for m in _queryIncidentPerson if m[3] is not None]))
                        if not _personId:
                            logger.error(f"Sin personas registradas para el incidente")
                            logger.error(f'Rechazado')
                            return False
                        _profe = 0 #4,5
                        _apoderado = 0 #15
                        _entrevistado = 0  #5
                        _entrevistador = 0 #6
                        for y in _refRoleType:
                            for z in _role:
                                if y == 5:
                                    _entrevistado += 1
                                if y == 6:
                                    _entrevistador += 1
                                if z == 4 or z == 5:
                                    _profe += 1
                                if z == 15:
                                    _apoderado += 1
                        if _entrevistado == 0:
                            logger.error(f'Sin entrevistado en reunion de incidente')
                            logger.error(f'Rechazado')
                            return False
                        if _entrevistador == 0:
                            logger.error(f'Sin entrevistador en reunion de incidente')
                            logger.error(f'Rechazado')
                            return False
                        if _profe == 0:
                            logger.error(f'Sin profesor asignado a incidente')
                            logger.error(f'Rechazado')
                            return False
                        if _apoderado == 0:
                            logger.error(f'Sin apoderado en incidente')
                            logger.error(f'Rechazado')
                            return False
                    else:
                        logger.error(f"Sin personas registradas para el incidente")
                        logger.error(f"Rechazado")
                        return False
                logger.info(f'Incidentes validados')
                logger.info(f'Aprobado')
                return True
            else:
                logger.error(f"S/Datos ")
                logger.error(f"Rechazado")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn8F2 WC ##

## Inicio fn2AA WC ##
  def fn2AA(self, conn):
        try:
            results = conn.execute("""
            select p.personId
            from Person p join PersonStatus ps on p.PersonId=ps.PersonId
            where p.RefVisaTypeId=6 and ps.StatusValue=1 and ps.RefPersonStatusTypeId=25;
            """).fetchall()

            resultsTwo = conn.execute("""
            select p.personId
            from Person p
            join PersonStatus ps on p.PersonId=ps.PersonId
            where p.RefVisaTypeId=6 and ps.RefPersonStatusTypeId=25;
            """).fetchall()

            if(len(results)>0 and len(resultsTwo)>0):
                lista = list(set([m[0] for m in results if m[0] is not None]))
                listaDos = list(set([m[0] for m in resultsTwo if m[0] is not None]))


                if lista == listaDos:
                    logger.info(f"todos los alumnos de intercambios fueron aprobados")
                    logger.info(f"Aprobado")
                    return True
                else :
                    logger.error(f'No todos los alumnos de intercambio han sido aprobados')
                    logger.error(f'Rechazado')
                    return False


            else:
                logger.error(f"Sin Datos")
                logger.error(f'Rechazado')
                return False

        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn2AA WC ##

## Inicio fn28A WC ##
  def fn28A(self, conn):
            try:
                _query = conn.execute("""
                select PB.PersonId,
                        PB.City,
                        PB.RefStateId,
                        PB.RefCountryId

                  from OrganizationPersonRole OPR
                          join Person P on OPR.PersonId = P.PersonId
                          join PersonIdentifier PI on P.PersonId = PI.PersonId
                          join PersonStatus PS on P.PersonId = PS.PersonId
                          join PersonBirthplace PB on P.PersonId = PB.PersonId
                  where PI.RefPersonIdentificationSystemId = 52
                    and OPR.RoleId = 6
                    and PI.Identifier is not null
                    and length(trim(PI.Identifier)) > 0
                    and PS.RefPersonStatusTypeId = 34;
                """).fetchall()
                if(len(_query)>0):
                  _personId = (list([m[0] for m in _query if m[0] is not None]))
                  if not _personId:
                    logger.error(f"Sin Person ID")
                    logger.error(f'Rechazado')
                    return False
                  _city = (list([m[1] for m in _query if m[1] is not None]))
                  if not _city:
                    logger.error(f"Sin ciudad de origen")
                    logger.error(f'Rechazado')
                    return False
                  _state = (list([m[2] for m in _query if m[2] is not None]))
                  if not _state:
                    logger.error(f"Sin estado de origen")
                    logger.error(f'Rechazado')
                    return False
                  _country = (list([m[3] for m in _query if m[3] is not None]))
                  if not _country:
                    logger.error(f"Sin Pais de origen")
                    logger.error(f'Rechazado')
                    return False
                  logger.info(f'Documento de pais de origen registrado correctamente')
                  logger.info(f'Aprobado')
                  return True
                else:
                    logger.error(f"S/Datos")
                    logger.error(f"Rechazado")
                    return False
            except Exception as e:
                logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                logger.error(f"Rechazado")
                return False
## Fin fn28A WC ##

## Inicio fn5E1 WC ##
  def fn5E1(self,conn):
        try:
            _query = conn.execute("""
            select OPR.OrganizationPersonRoleId,
                (SELECT count(OPR.PersonId)
                    from OrganizationPersonRole OPR
                            join Organization O on OPR.OrganizationId = O.OrganizationId
                            join Course C on O.OrganizationId = C.OrganizationId
                    where OPR.RoleId = 6
                    and O.RefOrganizationTypeId = 21) as MatriculasTotales
            from OrganizationPersonRole OPR
                    join Organization O on OPR.OrganizationId = O.OrganizationId
                    join Course C on O.OrganizationId = C.OrganizationId
            where OPR.RoleId = 6
            and O.RefOrganizationTypeId = 21
            group by OPR.OrganizationPersonRoleId;
            """).fetchall()
            if(len(_query)>0):
                _alumnos = (list([m[0] for m in _query if m[0] is not None]))
                if not _alumnos :
                    logger.error(f"Sin alumnos registrados")
                    logger.error(f'Rechazado')
                    return False
                _matriculasTotales = (list([m[1] for m in _query if m[1] is not None]))
                if not _matriculasTotales :
                    logger.error(f"Sin matriculas registradas")
                    logger.error(f'Rechazado')
                    return False
                _totalAlumnos = int(len(_alumnos))
                if int(_matriculasTotales[0]) == _totalAlumnos:
                        _queryRegistroAsistencia = conn.execute("""
                        select DISTINCT RoleAttendanceEventId,
                                        Date,
                                        RefAttendanceEventTypeId
                        from RoleAttendanceEvent
                        where RefAttendanceEventTypeId = 1 and Date is not null
                        group by date;
                        """).fetchall()
                        if(len(_queryRegistroAsistencia)>0):
                            logger.info(f'Matriculas registradas y asistencia diaria realizada')
                            logger.info(f'Aprobado')
                            return True
                        else:
                            logger.error(f'Asistencia diaria no realizada por el establecimiento')
                            logger.error(f'Rechazado')
                            return False
                else:
                    logger.error(f"Sin matriculas no coinciden con total de alumnos registrados")
                    logger.error(f'Rechazado')
                    return False
            else:
                logger.error(f'S/Datos')
                logger.error(f'Rechazado')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn531 WC ##

## Inicio fn28B WC ##
  def fn28B(self, conn):
        try:
            _query = conn.execute("""
            select
                PS.PersonStatusId,
                PS.PersonId,
                PS.RefPersonStatusTypeId,
                PS.docNumber,
                PS.Description,
                PS.fileScanBase64
            from OrganizationPersonRole OPR
                    join Person P on OPR.PersonId = P.PersonId
                    join PersonStatus PS on P.PersonId = PS.PersonId
                    join PersonBirthplace PB on P.PersonId = PB.PersonId
            where OPR.RoleId = 6
            and PB.RefCountryId != 45
            and PS.RefPersonStatusTypeId = 34
            group by P.PersonId;
            """).fetchall()
            if(len(_query)>0):
                _docNumber = (list([m[3] for m in _query if m[0] is not None]))
                if not _docNumber :
                    logger.error(f"Sin Nro de documento")
                    logger.error(f'Rechazado')
                    return False
                _description = (list([m[4] for m in _query if m[0] is not None]))
                if not _description :
                    logger.error(f"Sin descripcion de documento")
                    logger.error(f'Rechazado')
                    return False
                _fileScanBase64 = (list([m[5] for m in _query if m[0] is not None]))
                if not _fileScanBase64 :
                    logger.error(f"Sin fileScanBasse64")
                    logger.error(f'Rechazado')
                    return False
                logger.info(f'Certificado de convalidacion de ramos validado')
                logger.info(f'Aprobado')
                return True
            else:
                logger.error(f"S/Datos")
                logger.error(f"Rechazado")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn28B WC ##

## Fin fn9F2 WC ##
  def fn9F2(self, conn):
        try:
            queryEstudiantes = conn.execute("""
                select
          DISTINCT
          o.OrganizationId,
          o.Name
                from person p join OrganizationPersonRole opr
                on p.PersonId = opr.PersonId
                join Organization O on opr.OrganizationId = O.OrganizationId
                where opr.RoleId = 6 and O.RefOrganizationTypeId = 21;""").fetchall()

            if (len(queryEstudiantes)>0):
                organizations = (list([m[0] for m in queryEstudiantes if m[0] is not None]))
                organizations = str(organizations)
                organizations = organizations.replace('[','(')
                organizations = organizations.replace(']',')')
                querySelect = "select CourseId from CourseSection where CourseId in"
                queryComplete = querySelect+organizations
                try:
                    queryAsignaturas = conn.execute(queryComplete).fetchall()
                    if (len(queryAsignaturas)>0):
                        cursos = (list([m[0] for m in queryAsignaturas if m[0] is not None]))
                        cursos = str(cursos)
                        cursos = cursos.replace('[','(')
                        cursos = cursos.replace(']',')')
                        querySelectCalendar = "select * from OrganizationCalendar where OrganizationId in"
                        queryCalendarComplete = querySelectCalendar+cursos
                        try:
                            queryCalendarios = conn.execute(queryCalendarComplete).fetchall()
                            if (len(queryCalendarios)>0):
                                organizationId = (list([m[1] for m in queryCalendarios if m[0] is not None]))
                                calendarCode = (list([m[2] for m in queryCalendarios if m[0] is not None]))
                                calendarDescripction = (list([m[3] for m in queryCalendarios if m[0] is not None]))
                                calendarYear = (list([m[4] for m in queryCalendarios if m[0] is not None]))

                                if not organizationId :
                                    logger.error(f"Sin OrganizationId")
                                    logger.error(f'Rechazado')
                                    return False
                                if not calendarCode :
                                    logger.error(f"Sin CalendarCode")
                                    logger.error(f'Rechazado')
                                    return False
                                if not calendarDescripction :
                                    logger.error(f"Sin CaldendarDescription")
                                    logger.error(f'Rechazado')
                                    return False
                                if not calendarYear :
                                    logger.error(f"Sin CalendarYear")
                                    logger.error(f'Rechazado')
                                    return False
                                print(f'Calendarios ingresados correctamente')
                                print(f'Aprobado')
                                return True
                            else:
                                logger.error(f"S/Datos")
                                logger.error(f"Rechazado")
                                return False
                        except Exception as e:
                            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                            logger.error(f"Rechazado")
                            return False
                    else:
                        logger.error(f"S/Datos")
                        logger.error(f"Rechazado")
                        return False
                except Exception as e:
                    logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                    logger.error(f"Rechazado")
                    return False
            else:
                logger.error(f"S/Datos")
                logger.error(f"Rechazado")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn9F2 WC ##

## Inicio fn9F3 WC ##
  def fn9F3(self, conn):
        try:
            incident = conn.execute("""
            select IncidentId from Incident;
            """).fetchall()
            if(len(incident)>0):
                listIncident = (list([m[0] for m in incident if m[0] is not None]))
                for x in listIncident:
                    try:
                        x = str(x)
                        incidentParent = conn.execute("""
                        select * from IncidentPerson where IncidentId =
                        """+x+"""
                        where RefIncidentPersonRoleTypeId = 8
                        and RefIncidentPersonTypeId = 43
                        """).fetchall()
                        incidentProfessor = conn.execute("""
                        select * from IncidentPerson where IncidentId =
                        """+x+"""
                        where RefIncidentPersonRoleTypeId = 7
                        and RefIncidentPersonTypeId = 44
                        """).fetchall()
                        parent = 0
                        professor = 0
                        if (len(incidentParent)>0):
                            parent +=1
                        else:
                            logger.error(f"S/Datos")
                            logger.error(f"Rechazado")
                            return False
                        if (len(incidentProfessor)>0):
                            professor += 1
                        else:
                            logger.error(f"S/Datos")
                            logger.error(f"Rechazado")
                            return False
                    except Exception as e:
                        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                        logger.error(f"Rechazado")
                        return False
                logger.info(f'Reuniones validas')
                logger.info(f'Aprobado')
                return True
            else:
                logger.error(f"Sin incidentes registrados")
                logger.error(f"Aprobado")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn9F3 WC ##

### WebClass FIN ###


### Registro de Salidas y Retiros (No Habituales) Mi Aula INICIO ###

 ### FN0FA INICIO ###
  def fn0FA(self, conn):
    try:
      r_ = conn.execute("""
      SELECT A.personId
      FROM PersonList A
	    JOIN OrganizationPersonRole B
      ON A.personId = B.personId
	    JOIN jerarquiasList C
      ON B.OrganizationId = C.OrganizationIdDelCurso
      WHERE A.Role like '%Estudiante%'
      AND C.nivel NOT IN ('03:Educación Básica Adultos'
							            ,'06:Educación Media Humanístico Científica Adultos'
							            ,'08:Educación Media Técnico Profesional y Artística, Adultos');
    """).fetchall()

      logger.info(f"VERIFICA QUE EXISTA LISTADO DE ALUMNOS EN VISTA PERSONLIST Y QUE TENGAN PERSONAS ASOCIADAS Y AUTORIZADAS PARA RETIRO.")

      if(len(r_)>0):
        c_ = 0
        p_ = self.convertirArray2DToList(list([m[0] for m in r_ if m[0] is not None]))

        for a_ in p_:
          s_ = "SELECT RetirarEstudianteIndicador  FROM  PersonRelationship WHERE personId = '%s'" %(str(a_))
          t_ = conn.execute(s_).fetchall()
          r2_ = self.convertirArray2DToList(list([m[0] for m in t_ if m[0] is not None]))

          if(len(r2_)>0):
            for r in r2_:
              if(r == True):
                c_ = c_ + 1

        logger.info(f"Total Alumnos                                     : {len(r_)}")
        logger.info(f"Total Personas asociadas y autorizadas para retiro: {c_}")

        if(c_ == len(r_)):
          logger.info(f"TODOS los alumnos tienen informacion de personas asociadas y/o autorizadas para retiro.")
          logger.info(f"Apobado")
          return True
        else:
          logger.error(f"No todos los alumnos tienen informacion de personas asociadas y/o autorizadas para retiro.")
          logger.error(f"Rechazado")
          return False

      else:
        logger.info(f"S/Datos")
        return  False

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList filtrada por alumnos: {str(e)}")
      logger.error(f"Rechazado")
      return False
### FN0FA FIN ###

### FN0FB INICIO ###

  def fn0FB(self, conn):
    try:

      _msg = ""
      _f1 = False
      _drk = 0 #DigitalRandomKey
      _pr = 0 #PersonId autorizada para retiro

      _s1 = """SELECT a.OrganizationPersonRoleId
                      ,date(a.ExitDate) as ExitDate
                      ,time(a.ExitDate) as ExitTime
                      ,a.personId
                FROM OrganizationPersonRole a
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN RefOrganizationType D
                ON B.RefOrganizationTypeId = D.RefOrganizationTypeId
                WHERE a.RoleId = 6
                AND a.ExitDate IS NOT NULL
                AND B.name NOT IN ('03:Educación Básica Adultos'
                                    ,'06:Educación Media Humanístico Científica Adultos'
                                    ,'08:Educación Media Técnico Profesional y Artística, Adultos')
                AND D.Description = 'K12 School';"""

      _s2 = """SELECT a.OrganizationPersonRoleId
                      ,date(a.ExitDate) as ExitDate
                      ,time(a.ExitDate) as ExitTime
                      ,a.personId
                FROM OrganizationPersonRole a
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN RefOrganizationType C
                ON B.RefOrganizationTypeId = C.RefOrganizationTypeId
                WHERE date(a.ExitDate) = ?
                AND (c.Description = 'Course Section' or c.Description = 'Course');"""

      _s3 = """SELECT digitalRandomKey,
                      fileScanBase64,
                      observaciones
                FROM RoleAttendanceEvent
              WHERE OrganizationPersonRoleId = ?
                AND (RefAttendanceEventTypeId = 2 OR RefAttendanceEventTypeId = 5)
                AND RefAttendanceStatusId = 5
                AND date(Date) = ?
                and time(Date) = ?;"""

      _s4 = """SELECT a.OrganizationPersonRoleId
                      ,date(a.ExitDate) as ExitDate
                      ,time(a.ExitDate) as ExitTime
                      ,a.personId
                      ,a.RoleId
                FROM OrganizationPersonRole a
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN RefOrganizationType C
                ON B.RefOrganizationTypeId = C.RefOrganizationTypeId
                WHERE date(a.ExitDate) = ?
                AND time(a.ExitDate) = ?
                AND c.Description = 'K12 School';"""

      _s5 = """SELECT A.RelatedPersonId ,A.RetirarEstudianteIndicador
                    FROM PersonRelationship A
                    WHERE A.personId = ?"""

      #VERIFICA SI EXISTE REGISTRO DE RETIROS ANTICIPADOS DEL ESTABLECIMIENTO (OrganizationPersonRole)
      logger.info(f"VERIFICA CONSISTENCIA EN FECHA Y HORA DE REGISTROS DE RETIRO Y LA EXISTENCIA DE FIRMA DIGITAL Y/O DOCUMENTO DIGITALIZADO DE AUTORIZACION PARA EL CASO DE QUIEN RETIRA.")
      _r = conn.execute(_s1).fetchall()
      if(len(_r)>0):
        for r in _r:
          _o = r[0]
          _f = r[1]
          _t = r[2]
          _p = r[3]
          #VERIFICA SI EXISTE REGISTRO DE RETIRO DE CLASES PREVIO AL RETIRO DEL ESTABLECIMIENTO EN LA MISMA FECHA (OrganizationPersonRole)
          _v = (str(_f))
          _r2 = conn.execute(_s2, _v).fetchall()
          if (len(_r2)>0):
            for r2 in _r2:
              _o2 = r2[0]
              _f2 = r2[1]
              _t2 = r2[2]
              _p2 = r2[3]

              if(_p == _p2):
                _v2 = (str(_o2),str(_f2),str(_t2))
                _r3 = conn.execute(_s3, _v2).fetchall()
                if(len(_r3)>0):
                  for r3 in _r3:
                    _drk = r3[0]
                    if _drk is None:
                      logger.error(f"Registro de salida de clases no tiene firma de docente (OrganizationPersonRole).")
                      logger.error(f"Rechazado")
                      return False
                else:
                  logger.error(f"No hay registro de retiro de clases (RoleAttendanceEvent)")
                  logger.error(f"Rechazado")
                  return False

              else:
                _v2 = (str(_o2),str(_f2),str(_t2))
                _r3 = conn.execute(_s3, _v2).fetchall()
                if(len(_r3)>0):
                  for r3 in _r3:
                    _drk2 = r3[0]

                    if _drk2 is None:
                      logger.error(f"Registro de salida de clases no tiene firma de docente (RoleAttendanceEvent).")
                      logger.error(f"Rechazado")
                      return False
          else:
            logger.error(f"NO existe registro de salida de clases previo al retiro del establecimiento del alumno.")
            logger.error(f"Rechazado")
            return False

          #VERIFICA SI EXISTE REGISTRO DE RETIRO DE ESTABLECIMIENTO Y QUE COINCIDA CON FECHA Y HORA (RoleAttendanceEvent)
          _v4 = (str(_f),str(_t))
          _r4 = conn.execute(_s4, _v4).fetchall()
          if(len(_r4)>0):
            for r4 in _r4:
              _o4 = r4[0]
              _f4 = r4[1]
              _t4 = r4[2]
              _p4 = r4[3]
              _rl4 = r4[4]

              _v5 = (str(_o4),str(_f4),str(_t4))
              _r5 = conn.execute(_s3, _v5).fetchall()
              #logger.info(f"_o4 {str(_o4)},_f4 {str(_f4)},_t4 {str(_t4)},_rl {str(_rl4)}")
              if(len(_r5)>0):
                for r5 in _r5:
                  _drk = r5[0]
                  _fsb = r5[1]
                  _obs = r5[2]
                  if(_rl4 == 6):
                    if(_drk is None and _obs is None):
                      logger.error(f"Falta firma y observacion en registro de retiro de estudiante de establecimiento.")
                      logger.error(f"Rechazado")
                      return False
                  elif(_rl4 == 11):
                    if _drk is None:
                      logger.error(f"Falta firma de administrativo en registro de retiro de estudiante de establecimiento.")
                      logger.error(f"Rechazado")
                      return False
                  else:
                    _v6 = (_p4)
                    _r6 = conn.execute(_s5, _v6).fetchall()
                    if(len(_r6)>0):
                      for r6 in _r6:
                        _pr = r6[1]
                        if not _pr:
                          logger.info(f"La persona que retira a alumno no figura como autorizado en el sistema.")
                          logger.info(f"Apobado")
                          return True
                    if(_drk is None and _fsb is None):
                      logger.error(f"Falta firma o documento digitalizado de apoderado en registro de retiro de estudiante de establecimiento.")
                      logger.error(f"Rechazado")
                      return False

        return True
      else:
        logger.info(f"NO existen registros de retiro anticipado del establecimiento de alumnos.")
        logger.info(f"Apobado")
        return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de retiros anticipados: {str(e)}")
      logger.error(f"Rechazado")
      return False

### FN0FB FIN ###

### Registro de Salidas y Retiros (No Habituales) Mi Aula FIN ###