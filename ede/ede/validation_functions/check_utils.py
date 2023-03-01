from itertools import cycle
import re
import pandas as pd
from ede.ede._logger import logger
import json


def validateJSON(jsonData: str) -> bool:
    try:
        dictData = json.loads(jsonData)
        if(dictData.get("ArtículoProtocolo", None) is not None
           and dictData.get("Severidad", None) is not None
           and dictData.get("Procedimiento", None) is not None):
            return True
    except:
        pass
    return False


# https://drive.google.com/open?id=1vZD8ufVm3Z71V9TveQcLI0A02wrmwsz43z3TyWl9C-s
def cargarPlanillaConListasParaValidar():
    #idFile = '1vZD8ufVm3Z71V9TveQcLI0A02wrmwsz43z3TyWl9C-s'
    #url = f'https://docs.google.com/spreadsheets/d/{idFile}/export?format=xlsx&id={idFile}'
    url = './ede/ede/listValidationData.xlsx'
    xd = pd.read_excel(url, 'ListValidations')
    _t = f'Planilla {url} cargada satisfactoriamente'
    logger.info(_t)
    return xd


listValidations = cargarPlanillaConListasParaValidar()


def separaRUT(*args):
    if (len(args) > 0) and (args[0] is not None):
        dv = ''.join([c for c in list(args[0].upper()) if c.isalpha()])
        aux = ''.join([c for c in list(args[0]) if c.isdigit()])
        if(dv == ''):
            dv = aux[-1:]
            aux = aux[:-1]
    else:
        aux = 0
        dv = 0
    return aux, dv


def validarRut(aux, dv):
    revertido = map(int, reversed(str(aux)))
    factors = cycle(range(2, 8))
    s = sum(d * f for d, f in zip(revertido, factors))
    res = (-s) % 11
    if ((str(res) == dv) or (dv == "K" and res == 10)):
        return True
    return False

# VERIFICA SI EL RUT INGRESADO ES VALIDO


def validarRUN(*args):
    aux, dv = separaRUT(*args)
    if(aux != 0 and dv != 0):
        if(validarRut(aux, dv)):
            if(int(aux) <= 47000000):
                return True
    return False


def validarIpe(*args):
    aux, dv = separaRUT(*args)
    if(aux != 0 and dv != 0):
        if(validarRut(aux, dv)):
            if(int(aux) >= 100000000):
                return True
    return False


def convertirArray2DToList(text):
    _l = []
    for e in text:
        if "|" not in str(e):
            _l.append(e)
        else:
            for subE in e.split("|"):
                _l.append(subE)
    return _l


def imprimeErrores(lista, fn, msg):
    _l = convertirArray2DToList(lista)
    _err = set([e for e in _l if not fn(e)])
    _r = False if len(_err) > 0 else True
    _t = f"{msg}: {_r}. {_err}"
    logger.info(_t)
    return _err, _r


def validaBoolean(e):
    return e

    # VERIFICA DATOS DE LAS ORGANIZACIONES
    # VERIFICA JERARQUIA DE LOS DATOS
    # la jerarquí es:
    #  RBD -> Modalidad -> Jornada -> Niveles -> Rama ->
    #  Sector Económico -> Especialidad ->
    #  Tipo de Curso -> COD_ENSE -> Grado -> Curso -> Asignatura


def validaFormatoRBD(e):
    r = re.compile('^RBD\d{5}$')
    if(isinstance(e, str)):
        return r.match(e) is not None
    return False


def validaFormatoE164Telefono(e):
    r = re.compile('^\+56\d{9,15}$')
    if(isinstance(e, str)):
        return r.match(e) is not None
    return False


def validaFormatoNumero(e):
    r = re.compile('^\d{0,4}$')
    if(isinstance(e, str)):
        return r.match(e) is not None
    return False


def validaFormatoFecha(e):
    r = re.compile(
        '^((19|20)(\d{2})-(1[0-2]|0?[0-9])-([12][0-9]|3[01]|0?[1-9]))[ T]?((0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.\d{0,})?)?([+-](0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))?$')
    if(isinstance(e, str)):
        return r.match(e) is not None
    return False


def validaTribalAffiliation(e):
    _lista = list(set(listValidations['TribalAffiliationDescriptionList']))
    return True if e in _lista else False


def validaModalidad(e):
    _lista = list(set(listValidations['modalidadesList']))
    return True if e in _lista else False


def validaJornada(e):
    _lista = list(set(listValidations['jornadasList']))
    return True if e in _lista else False


def validaNivel(e):
    _lista = list(set(listValidations['nivelList']))
    return True if e in _lista else False


def validaRama(e):
    _lista = list(set(listValidations['ramaList']))
    return True if e in _lista else False


def validaSector(e):
    _lista = list(set(listValidations['sectorList']))
    return True if e in _lista else False


def validaEspecialidad(e):
    _lista = list(set(listValidations['especialidadList']))
    return True if e in _lista else False


def validaTipoCurso(e):
    _lista = list(set(listValidations['tipoCursoList']))
    return True if e in _lista else False


def validaCodigoEnse(e):
    _lista = list(set(listValidations['codigoEnseList']))
    return True if e in _lista else False


def validaGrado(e):
    _lista = list(set(listValidations['gradoList']))
    return True if e in _lista else False


def validaLetraCurso(e):
    r = re.compile('^[A-Z]{1,2}$')
    if(isinstance(e, str)):
        return r.match(e) is not None
    return False


def validaFormatoClaveAleatoria(e):
    r = re.compile('^[0-9]{6}([-]{1}[0-9kK]{1})?$')
    if(isinstance(e, str)):
        return r.match(e) is not None
    return False


def validaEventosDeAsistencia(e):
    _r = False
    if (e[1] == 'Class/section attendance' and e[2] == 'Course Section'):
        _r = True
    elif (e[1] == 'Daily attendance' and e[2] == 'Course'):
        _r = True
    elif (e[0] == 'Reingreso autorizado' and e[2] == 'K12 School'):
        _r = True
    elif (e[1] == 'Asistencia al establecimiento' and e[2] == 'K12 School'):
        _r = True
    return _r
