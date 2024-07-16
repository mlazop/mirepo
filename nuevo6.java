package pe.gob.sunat.contribuyente.cpe.facturagem.batch.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Date;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import pe.gob.sunat.contribuyente.cpe.facturagem.core.model.ApplicationResponse;
import pe.gob.sunat.contribuyente.cpe.facturagem.core.model.ConstanciaComprobacion;
import pe.gob.sunat.contribuyente.cpe.facturagem.core.model.PublicarConstanciaBean;
import pe.gob.sunat.contribuyente.cpe.facturagem.core.model.PublicarRespaldoBean;
import pe.gob.sunat.contribuyente.cpe.facturagem.core.model.ConstanciaComprobacion.SunatInfo;
import pe.gob.sunat.contribuyente.cpe.facturagem.dao.DAELogCabUpdateDeleteDAO;
import pe.gob.sunat.contribuyente.cpe.facturagem.dao.DAELogPackUpdateDeleteDAO;
import pe.gob.sunat.contribuyente.cpe.facturagem.dao.FELogCabUpdateDeleteDAO;
/* INI DCERRENO 03FEB2017 */
// import pe.gob.sunat.contribuyente.cpe.facturagem.dao.FELogDetInsertDAO;
/* FIN DCERRENO 03FEB2017 */
import pe.gob.sunat.contribuyente.cpe.facturagem.dao.FELogPackUpdateDeleteDAO;
import pe.gob.sunat.contribuyente.cpe.facturagem.dao.T7979CdrOseInsertDAO;
import pe.gob.sunat.contribuyente.cpe.facturagem.model.BillDaeLogCab;
import pe.gob.sunat.contribuyente.cpe.facturagem.model.BillDaeLogPack;
import pe.gob.sunat.contribuyente.cpe.facturagem.model.BillLogCab;
import pe.gob.sunat.contribuyente.cpe.facturagem.model.BillLogPack;
import pe.gob.sunat.contribuyente.cpe.facturagem.service.BillDAEStoreServiceImpl;
import pe.gob.sunat.contribuyente.cpe.facturagem.service.BillStoreService;
import pe.gob.sunat.contribuyente.cpe.facturagem.util.FacturaUtils;
import pe.gob.sunat.contribuyente2.boleta.resumen.bean.T7979CdrOse;
import pe.gob.sunat.tecnologia2.kafka.EnvioKafkaGemAuditoria;
import pe.gob.sunat.tecnologia2.kafka.EnvioKafkaGemComprobante;

public class RespaldoServiceImpl implements RespaldoService {

    private static final Logger log = LoggerFactory.getLogger(RespaldoServiceImpl.class);

    // private Base64 base64Decoder = new Base64();
    @Autowired
    private BillStoreService storeService;
    @Autowired
    @Qualifier("UpdateLogCab")
    private FELogCabUpdateDeleteDAO updateLogCabDAO;
    @Autowired
    @Qualifier("UpdateLogPack")
    private FELogPackUpdateDeleteDAO updateLogPackDAO;
    
    @Autowired
    @Qualifier("InsertT7979")
    private T7979CdrOseInsertDAO cdrOseDAO;
    
    ///INICIO - PAS20191U210100214
    @Autowired
    @Qualifier("send.daeStore")
    private BillDAEStoreServiceImpl storeDaeService;
    @Autowired
    @Qualifier("UpdateDaeLogCab")
    private DAELogCabUpdateDeleteDAO updateDaeLogCabDAO;
    @Autowired
    @Qualifier("UpdateDaeLogPack")
    private DAELogPackUpdateDeleteDAO updateDaeLogPackDAO;
    ///FIN - PAS20191U210100214
    

    public void setStoreService(BillStoreService storeService) {
        this.storeService = storeService;
    }

    @Override
    @TransactionAttribute(value = TransactionAttributeType.REQUIRED)
    public void respaldarComprobante(PublicarRespaldoBean mensaje) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("RespaldoServiceImpl.respaldarComprobante - Inic");
            log.debug("Inicio del respaldo del comprobante. [ticket:" + mensaje.getTicket() + ", usuario:"
                    + mensaje.getUsuario() + "]");
        }
        // 1.- Grabamos el archivo zip del contribuyente
        
        try {
            byte[] datosArchivo = Base64.decodeBase64(mensaje.getArchivoContenido().getBytes());
            
            /*
            ///INICIO - PAS20191U210100214
            String[] cadenaName = mensaje.getArchivoNombre().split("-");
    	    if (cadenaName.length > 2) {
    			String codAdq = "30";
    			String codOpe = "34";
    			String codAdqOtr = "42";
    			log.info("RespaldoServiceImpl.respaldarComprobante - codCPE: " + cadenaName[1].trim());
    			if (cadenaName[1].trim().equals(codAdq) || cadenaName[1].trim().equals(codOpe) || cadenaName[1].trim().equals(codAdqOtr)) {
    				log.info("RespaldoServiceImpl.respaldarComprobante - Inicio de saveInput de DAE");
    				
    				storeDaeService.saveInput(mensaje.getTicket().toString(), datosArchivo,
    	                    mensaje.getArchivoNombre(), mensaje.getUsuario());
    			} else {
    				log.info("RespaldoServiceImpl.respaldarComprobante - Inicio de saveInput");
    				storeService.saveInput(mensaje.getTicket().toString(), mensaje.getCorrelativo(), datosArchivo,
    	                    mensaje.getArchivoNombre(), mensaje.getUsuario());
    			}
    		} else {
    			storeService.saveInput(mensaje.getTicket().toString(), mensaje.getCorrelativo(), datosArchivo,
                        mensaje.getArchivoNombre(), mensaje.getUsuario());
    		}
            
            ///FIN - PAS20191U210100214
             * 
             */
            
            String fechaArchivo= null;
            /*try {
                InetAddress localMachine = InetAddress.getLocalHost();
                String hostname = localMachine.getHostName();
                String ip = localMachine.getHostAddress();
                if (log.isDebugEnabled()) {
                    log.debug("Servidor: " + hostname + " - IP: " + ip);
                }
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error al obtener el nombre del servidor o la IP: " + e.getMessage());
                }
            }*/
            
            
            try {
            	
            	
	            ByteArrayInputStream entrada = new ByteArrayInputStream(datosArchivo);
	            ZipInputStream zipInput = new ZipInputStream(entrada);
 
	            ZipEntry entradaZip;
	            while ((entradaZip =zipInput.getNextEntry()) != null) {
	                ByteArrayOutputStream salida = new ByteArrayOutputStream();
	                byte[] buffer = new byte[1024];
	                int longitud;
	                while ((longitud = zipInput.read(buffer)) > 0) {
	                    salida.write(buffer, 0, longitud);
	                }
	                byte[] datosDescomprimidos = salida.toByteArray();
 	                salida.close();
 	                String archivoDescomprimido = new String(datosDescomprimidos);
 	                String cadXml = "<cbc:IssueDate>\\s*(\\d{4}-\\d{2}-\\d{2})\\s*</cbc:IssueDate>";
	                Pattern pattern = Pattern.compile(cadXml);
	                Matcher matcher = pattern.matcher(archivoDescomprimido);
 
	                if (matcher.find()) {
	                    fechaArchivo = matcher.group(1);
	                    if (log.isDebugEnabled()) {log.debug("FechaArchivo: " + fechaArchivo);}
	                } else {
	                	if (log.isDebugEnabled()) {log.debug("No se encontró la fecha del archivo en el contenido descomprimido.");}
	                }

	    			String [] archivoNombre = (mensaje.getArchivoNombre()).split("-");
	    			String numRuc = archivoNombre[0];
	    			String codCpe = archivoNombre[1];
	    			String numSerie = archivoNombre[2];
	    			String numCpe = archivoNombre[3];
	    			String [] fecha = null;
	    			String annio = null;
	    			String mes = null;
	    			String dia = null;
	    			if (fechaArchivo != null) {
	    				fecha = fechaArchivo.split("-");
	    				annio = fecha[0];
	    				mes = fecha[1];
	    				dia = fecha[2];
	    			}
	    			char ultimoDigitoRuc = numRuc.charAt(numRuc.length() - 1);
	    			int inicio = Math.max(0, numRuc.length() - 5); 
	    			String cuatroDigitosAnterioresRuc = numRuc.substring(inicio, numRuc.length() - 1);
	    			String numTicket = mensaje.getTicket().toString();
	    			String nomArchivo=numRuc + "-"+ codCpe + "-" + numSerie + "-" + numCpe;
	    			if (log.isDebugEnabled()) {log.debug("nomArchivo: "+nomArchivo);}
	    			String rutaBase="/Tmp/test/";
	    			String ruta = rutaBase+"/"+annio +"/"+mes+"/"+dia+"/"+ultimoDigitoRuc+"/"+cuatroDigitosAnterioresRuc+"/"+numRuc+"/"+numTicket+"/"+codCpe+"/1/";
	    			if (log.isDebugEnabled()) {log.debug("ruta: "+ruta);}

	    			
	    			try {
	    			    Path directorio = Paths.get(ruta);
	    			    if (!Files.exists(directorio)) {
	    			        try {
	    			            Files.createDirectories(directorio);
	    			            if (log.isDebugEnabled()) {
	    			                log.debug("Directorio creado: " + directorio);
	    			            }
	    			        } catch (IOException e) {
	    			            if (log.isDebugEnabled()) {
	    			                log.debug("Error al crear directorio: " + e.getMessage());
	    			            }
	    			            return;
	    			        }
	    			    } else {
	    			        if (!Files.isDirectory(directorio)) {
	    			            if (log.isDebugEnabled()) {
	    			                log.debug("La ruta especificada no es un directorio válido: " + ruta);
	    			            }
	    			            return;
	    			        }
	    			    }

	    			    String archivoCompleto = Paths.get(ruta, nomArchivo).toString();
	    			    try  {
	    			    	FileOutputStream fos = new FileOutputStream(archivoCompleto);
	    			        fos.write(datosArchivo);
	    			        if (log.isDebugEnabled()) {
	    			            log.debug("Archivo creado correctamente en: " + archivoCompleto);
	    			        }
	    			    } catch (IOException e) {
	    			        if (log.isDebugEnabled()) {
	    			            log.debug("Error al escribir en el archivo: " + e.getMessage());
	    			        }
	    			    }
	    			} catch (InvalidPathException e) {
	    			    if (log.isDebugEnabled()) {
	    			        log.debug("Ruta no válida: " + e.getMessage());
	    			    }
	    			} catch (SecurityException e) {
	    			    if (log.isDebugEnabled()) {
	    			        log.debug("No se tienen permisos para realizar la operación: " + e.getMessage());
	    			    }
	    			} 
	            }
	            zipInput.close();
	            entrada.close();
 
	        } catch (IOException e) {
	        	if (log.isDebugEnabled()) {log.debug("IOException: "+e);}
	        }
            
     
            
            
        } catch (org.springframework.dao.DuplicateKeyException e) {
            log.error("ya se respaldo el comprobante: " + mensaje.getTicket() + ", mensaje error: " + e.getMessage());
        }
        
        if (log.isDebugEnabled()) log.debug("RespaldoServiceImpl.respaldarComprobante - Fin");
    }

    @Override
    @TransactionAttribute(value = TransactionAttributeType.REQUIRED)
    public void respaldarConstancia(PublicarConstanciaBean mensaje) throws Exception {
    	
 
	
	if (log.isDebugEnabled()) log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - Inic");
	
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - hito" + mensaje.getHito());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - fault"+ mensaje.getFault() );
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - isZipped" + mensaje.isZipped());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - archivocontenido"+ mensaje.getArchivoContenido() );
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - archivoNombre" + mensaje.getArchivoNombre());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - correlativo" + mensaje.getCorrelativo());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - usuario"+ mensaje.getUsuario());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - sunatCdrInfo"+ mensaje.getSunatCdrInfo());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - datetimeEnqueue"+ mensaje.getDatetimeEnqueue());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - datetimeTicket"+ mensaje.getDatetimeTicket());
		log.debug("RespaldoServiceImpl.respaldarConstancia - RespaldoServiceImpl.respaldarConstancia - datetimeInitprocess"+ mensaje.getDatetimeInitprocess());
	
        ApplicationResponse response = mensaje.getApplicationResponse();
        log.debug("RespaldoServiceImpl.respaldarConstancia - nro ticket: " + response.getTicket().toString().trim());
        log.debug("RespaldoServiceImpl.respaldarConstancia - nro ticket: " + response.getTicket());

        if (response.getTicket() == null || "".equals(response.getTicket().toString().trim())) {
            log.warn("RespaldoServiceImpl.respaldarConstancia - No se envio el nro. de ticket. [ticket: null, codeError:" + response.getCodeError() + ", numHito:"
                    + mensaje.getHito() + ", withException:" + response.isWithException() + ", archivoNombre:"
                    + mensaje.getArchivoNombre() + ", fault:" + mensaje.getFault() + ", usuario:" + mensaje.getUsuario()
                    + "]");
            return;
        }
        
        
        if (log.isDebugEnabled()) {
            log.debug("RespaldoServiceImpl.respaldarConstancia - Inicio del respaldo de la constancia. [ticket:" + response.getTicket() + ", codeError:"
                    + response.getCodeError() + ", withException:" + response.isWithException() + ", archivoNombre:"
                    + mensaje.getArchivoNombre() + ", fault:" + mensaje.getFault() + ", usuario:" + mensaje.getUsuario()
                    + "]");
            log.debug("RespaldoServiceImpl.respaldarConstancia - CDR (0 = ok) : " + response.getCodeError());
        }
        
        ///INICIO - PAS20191U210100214
        boolean DaeCPE = false;
        if(!mensaje.getArchivoNombre().trim().equals("") ) {
	        log.debug("RespaldoServiceImpl.respaldarConstancia - ArchivoNombre: " + mensaje.getArchivoNombre());
	        String[] cadenaName = mensaje.getArchivoNombre().split("-");
	        String codAdq = "30";
			String codOpe = "34";
			String codAdqOtr = "42";
			log.debug("RespaldoServiceImpl.respaldarConstancia - codCPE: " + cadenaName[1].trim());
			DaeCPE = (cadenaName.length > 2) && cadenaName[1].trim().equals(codAdq) || cadenaName[1].trim().equals(codOpe) || cadenaName[1].trim().equals(codAdqOtr);
			log.debug("RespaldoServiceImpl.respaldarConstancia - es DAE: " + DaeCPE);
        }
        String numTicket = response.getTicket().toString();
        
        // 1.- Grabamos el archivo zip de la constancia
        log.debug("RespaldoServiceImpl.respaldarConstancia - numTicket " + numTicket);
        log.debug("RespaldoServiceImpl.respaldarConstancia - es mensaje.getFault() " + mensaje.getFault());
        
        if (!mensaje.getFault()) {
            try {
                String fileName = mensaje.getArchivoNombre();

                if (!"R".equals(fileName.substring(0, 1))) {
                    fileName = "R-" + fileName;
                }

                log.debug("RespaldoServiceImpl.mensaje.getArchivoContenido(): " + mensaje.getArchivoContenido());
                byte[] datosArchivo = Base64.decodeBase64(mensaje.getArchivoContenido().getBytes());
                if (datosArchivo == null || datosArchivo.length == 0) {
                    return;
                }

                if (!mensaje.isZipped()) {

                    if (log.isDebugEnabled()) {
                        log.debug("RespaldoServiceImpl.respaldarConstancia - La constancia no esta en formato zip. " + new String(datosArchivo));
                    }

                    datosArchivo = FileZipUtil.zipBytes(fileName, datosArchivo);

                }

                if (!"20131312955SUNAT".equals(mensaje.getUsuario())) {
                    
                    if(mensaje.getSunatCdrInfo() != null) {
                        
                        T7979CdrOse t7979CdrOseBean  = new T7979CdrOse();
                     // info sunat (informacion no conocida en primera instancia)
                        ConstanciaComprobacion.SunatInfo sunatInfo = mensaje.getSunatCdrInfo();
                        
                        //fec_cdr_sunat
                        t7979CdrOseBean.setFecCdr(new Date());
                            
                        //des_hash_cdr_sunat
                        t7979CdrOseBean.setDesHashCdr(sunatInfo.getHashCdr());
                            
                        //num_certif_sunat
                        t7979CdrOseBean.setNumCertifSunat(sunatInfo.getNumSerieCertificado());
                        
                        //num_arc_cdr_sunat
                        t7979CdrOseBean.setNumArcCdr(response.getTicket());
                        
                       //num_arc_cdr_ose    
                        t7979CdrOseBean.setNumArcCdrOse(response.getTicket());
                        
                        //
                        t7979CdrOseBean.setNumTicket(FacturaUtils.genTicketFromTicketAndCorrel(response.getTicket(),mensaje.getCorrelativo()));
                            
                        // auditoria
                        t7979CdrOseBean.setCodUsuregis(mensaje.getUsuario());
                        t7979CdrOseBean.setFecRegis(new Date()); 
                        
                        cdrOseDAO.updateCdrSunat(t7979CdrOseBean);
                    }
                    
                  //Inicio NFS
					String fechaArchivo= null;
			        try {
			            // Lectura de los datos comprimidos
			            ByteArrayInputStream entrada = new ByteArrayInputStream(datosArchivo);
			            ZipInputStream zipInput = new ZipInputStream(entrada);

			            // Descomprimir el archivo
			            ZipEntry entradaZip;
			            while ((entradaZip =zipInput.getNextEntry()) != null) {
			                // Crear un arreglo de bytes para almacenar los datos descomprimidos
			                ByteArrayOutputStream salida = new ByteArrayOutputStream();
			                byte[] buffer = new byte[1024];
			                int longitud;

			                // Leer datos del archivo comprimido y escribirlos en el flujo de salida
			                while ((longitud = zipInput.read(buffer)) > 0) {
			                    salida.write(buffer, 0, longitud);
			                }

			                // Convertir los datos descomprimidos a un arreglo de bytes
			                byte[] datosDescomprimidos = salida.toByteArray();

			                // Cerrar el flujo de salida
			                salida.close();

			                // Convertir los datos descomprimidos a una cadena de texto
			                String archivoDescomprimido = new String(datosDescomprimidos);

			                // Obtener la fecha del archivo
			                String cadXml = "<cbc:IssueDate>\\s*(\\d{4}-\\d{2}-\\d{2})\\s*</cbc:IssueDate>";
			                Pattern pattern = Pattern.compile(cadXml);
			                Matcher matcher = pattern.matcher(archivoDescomprimido);

			                if (matcher.find()) {
			                    fechaArchivo = matcher.group(1);
			                    if (log.isDebugEnabled()) {log.debug("FechaArchivo: " + fechaArchivo);}
			                } else {
			                	if (log.isDebugEnabled()) {log.debug("No se encontró la fecha del archivo en el contenido descomprimido.");}
			                }
			    			// Obtener la ruta a partir de la fecha y del nombre del archivo 
			    			String [] archivoNombre = (mensaje.getArchivoNombre()).split("-");
			    			String numRuc = archivoNombre[0];
			    			String codCpe = archivoNombre[1];
			    			String numSerie = archivoNombre[2];
			    			String numCpe = archivoNombre[3];
			    			
			    			String [] fecha = null;
			    			String annio = null;
			    			String mes = null;
			    			String dia = null;
			    		
			    			if (fechaArchivo != null) {
			    				fecha = fechaArchivo.split("-");
			    				annio = fecha[0];
			    				mes = fecha[1];
			    				dia = fecha[2];
			    			}
			                
			    			char ultimoDigitoRuc = numRuc.charAt(numRuc.length() - 1);
			    			int inicio = Math.max(0, numRuc.length() - 5); 
			    			String cuatroDigitosAnterioresRuc = numRuc.substring(inicio, numRuc.length() - 1);
			    			numTicket = response.getTicket().toString();
			    			String nomArchivo=numRuc + "-"+ codCpe + "-" + numSerie + "-" + numCpe;
			    			if (log.isDebugEnabled()) {log.debug("nomArchivo: "+nomArchivo);}
			    			
			    			String rutaBase="/Tmp/test";
			    			
			    			String ruta = rutaBase+"/"+annio +"/"+mes+"/"+dia+"/"+ultimoDigitoRuc+"/"+cuatroDigitosAnterioresRuc+"/"+numRuc+"/"+numTicket+"/"+codCpe+"/0/";
			    			if (log.isDebugEnabled()) {log.debug("ruta: "+ruta);}
			    			
			    			
			    			try {
			    			    Path directorio = Paths.get(ruta);
			    			    
			    			    // Verificar y crear directorio si no existe
			    			    if (!Files.exists(directorio)) {
			    			        try {
			    			            Files.createDirectories(directorio);
			    			            if (log.isDebugEnabled()) {
			    			                log.debug("Directorio creado: " + directorio);
			    			            }
			    			        } catch (IOException e) {
			    			            if (log.isDebugEnabled()) {
			    			                log.debug("Error al crear directorio: " + e.getMessage());
			    			            }
			    			            return;
			    			        }
			    			    } else {
			    			        if (!Files.isDirectory(directorio)) {
			    			            if (log.isDebugEnabled()) {
			    			                log.debug("La ruta especificada no es un directorio válido: " + ruta);
			    			            }
			    			            return;
			    			        }
			    			    }
			    			    
			    			    // Crear el archivo en el directorio especificado
			    			    String archivoCompleto = Paths.get(ruta, nomArchivo).toString();
			    			    try  {
			    			    	FileOutputStream fos = new FileOutputStream(archivoCompleto);
			    			        fos.write(datosArchivo);
			    			        if (log.isDebugEnabled()) {
			    			            log.debug("Archivo CDR creado correctamente en: " + archivoCompleto);
			    			        }
			    			    } catch (IOException e) {
			    			        if (log.isDebugEnabled()) {
			    			            log.debug("Error al escribir en el archivo: " + e.getMessage());
			    			        }
			    			    }
			    			} catch (InvalidPathException e) {
			    			    if (log.isDebugEnabled()) {
			    			        log.debug("Ruta no válida: " + e.getMessage());
			    			    }
			    			} catch (SecurityException e) {
			    			    if (log.isDebugEnabled()) {
			    			        log.debug("No se tienen permisos para realizar la operación: " + e.getMessage());
			    			    }
			    			} 
			    			
			            }
			            // Cerrar el flujo de entrada
			            zipInput.close();
			            entrada.close();

			        } catch (IOException e) {
			        	if (log.isDebugEnabled()) {log.debug("IOException: "+e);}
			        }
					//Fin NFS
                    
                     ///INICIO - PAS20191U210100214
                    
            	/*    if (DaeCPE) {
            	    	storeDaeService.saveOutputCpe(numTicket, datosArchivo,
                                mensaje.getArchivoNombre().replace(".xml", ".zip"), mensaje.getUsuario());
            		} else {
            			storeService.saveOutputCpe(numTicket, mensaje.getCorrelativo(), datosArchivo,
                                mensaje.getArchivoNombre(), mensaje.getUsuario());
            		}
                    ///FIN - PAS20191U210100214
                    */
                } else {
                	
                	
                	//Inicio NFS
					String fechaArchivo= null;
			        try {
			            // Lectura de los datos comprimidos
			            ByteArrayInputStream entrada = new ByteArrayInputStream(datosArchivo);
			            ZipInputStream zipInput = new ZipInputStream(entrada);

			            // Descomprimir el archivo
			            ZipEntry entradaZip;
			            while ((entradaZip =zipInput.getNextEntry()) != null) {
			                // Crear un arreglo de bytes para almacenar los datos descomprimidos
			                ByteArrayOutputStream salida = new ByteArrayOutputStream();
			                byte[] buffer = new byte[1024];
			                int longitud;

			                // Leer datos del archivo comprimido y escribirlos en el flujo de salida
			                while ((longitud = zipInput.read(buffer)) > 0) {
			                    salida.write(buffer, 0, longitud);
			                }

			                // Convertir los datos descomprimidos a un arreglo de bytes
			                byte[] datosDescomprimidos = salida.toByteArray();

			                // Cerrar el flujo de salida
			                salida.close();

			                // Convertir los datos descomprimidos a una cadena de texto
			                String archivoDescomprimido = new String(datosDescomprimidos);

			                // Obtener la fecha del archivo
			                String cadXml = "<cbc:IssueDate>\\s*(\\d{4}-\\d{2}-\\d{2})\\s*</cbc:IssueDate>";
			                Pattern pattern = Pattern.compile(cadXml);
			                Matcher matcher = pattern.matcher(archivoDescomprimido);

			                if (matcher.find()) {
			                    fechaArchivo = matcher.group(1);
			                    if (log.isDebugEnabled()) {log.debug("FechaArchivo: " + fechaArchivo);}
			                } else {
			                	if (log.isDebugEnabled()) {log.debug("No se encontró la fecha del archivo en el contenido descomprimido.");}
			                }
			    			// Obtener la ruta a partir de la fecha y del nombre del archivo 
			    			String [] archivoNombre = (mensaje.getArchivoNombre()).split("-");
			    			String numRuc = archivoNombre[0];
			    			String codCpe = archivoNombre[1];
			    			String numSerie = archivoNombre[2];
			    			String numCpe = archivoNombre[3];
			    			
			    			String [] fecha = null;
			    			String annio = null;
			    			String mes = null;
			    			String dia = null;
			    		
			    			if (fechaArchivo != null) {
			    				fecha = fechaArchivo.split("-");
			    				annio = fecha[0];
			    				mes = fecha[1];
			    				dia = fecha[2];
			    			}
			                
			    			char ultimoDigitoRuc = numRuc.charAt(numRuc.length() - 1);
			    			int inicio = Math.max(0, numRuc.length() - 5); 
			    			String cuatroDigitosAnterioresRuc = numRuc.substring(inicio, numRuc.length() - 1);
			    			numTicket = response.getTicket().toString();
			    			String nomArchivo=numRuc + "-"+ codCpe + "-" + numSerie + "-" + numCpe;
			    			if (log.isDebugEnabled()) {log.debug("nomArchivo: "+nomArchivo);}
			    			
			    			String rutaBase="/Tmp/test";
			    			
			    			String ruta = rutaBase+"/"+annio +"/"+mes+"/"+dia+"/"+ultimoDigitoRuc+"/"+cuatroDigitosAnterioresRuc+"/"+numRuc+"/"+numTicket+"/"+codCpe+"/0/";
			    			if (log.isDebugEnabled()) {log.debug("ruta: "+ruta);}
			    			
			    			
			    			try {
			    			    Path directorio = Paths.get(ruta);
			    			    
			    			    // Verificar y crear directorio si no existe
			    			    if (!Files.exists(directorio)) {
			    			        try {
			    			            Files.createDirectories(directorio);
			    			            if (log.isDebugEnabled()) {
			    			                log.debug("Directorio creado: " + directorio);
			    			            }
			    			        } catch (IOException e) {
			    			            if (log.isDebugEnabled()) {
			    			                log.debug("Error al crear directorio: " + e.getMessage());
			    			            }
			    			            return;
			    			        }
			    			    } else {
			    			        if (!Files.isDirectory(directorio)) {
			    			            if (log.isDebugEnabled()) {
			    			                log.debug("La ruta especificada no es un directorio válido: " + ruta);
			    			            }
			    			            return;
			    			        }
			    			    }
			    			    
			    			    // Crear el archivo en el directorio especificado
			    			    String archivoCompleto = Paths.get(ruta, nomArchivo).toString();
			    			    try  {
			    			    	FileOutputStream fos = new FileOutputStream(archivoCompleto);
			    			        fos.write(datosArchivo);
			    			        if (log.isDebugEnabled()) {
			    			            log.debug("Archivo CDR creado correctamente en: " + archivoCompleto);
			    			        }
			    			    } catch (IOException e) {
			    			        if (log.isDebugEnabled()) {
			    			            log.debug("Error al escribir en el archivo: " + e.getMessage());
			    			        }
			    			    }
			    			} catch (InvalidPathException e) {
			    			    if (log.isDebugEnabled()) {
			    			        log.debug("Ruta no válida: " + e.getMessage());
			    			    }
			    			} catch (SecurityException e) {
			    			    if (log.isDebugEnabled()) {
			    			        log.debug("No se tienen permisos para realizar la operación: " + e.getMessage());
			    			    }
			    			} 
			    			
			            }
			            // Cerrar el flujo de entrada
			            zipInput.close();
			            entrada.close();

			        } catch (IOException e) {
			        	if (log.isDebugEnabled()) {log.debug("IOException: "+e);}
			        }
					//Fin NFS
                	
                	/*
                	///INICIO - PAS20191U210100214
            	    if (DaeCPE) {
						log.info("RespaldoServiceImpl.respaldarConstancia - Inicio de saveInput de DAE");
						storeDaeService.saveOutputCpe(numTicket, datosArchivo,
								mensaje.getArchivoNombre().replace(".xml", ".zip"), mensaje.getUsuario());
            		} else {
            			storeService.saveOutputCpe(numTicket, mensaje.getCorrelativo(), datosArchivo,
                                mensaje.getArchivoNombre(), mensaje.getUsuario());
            		}
                    ///FIN - PAS20191U210100214
					*/
                }

            } catch (org.springframework.dao.DuplicateKeyException e) {
                // el comprobante ya fue registrado
                log.error("RespaldoServiceImpl.respaldarConstancia - Ya se registro la constancia: ticket " + response.getTicket(), e);

                return;
            }

        }
        
        if (DaeCPE) {
        	// 2.- Grabamos en las tablas del log
            BillDaeLogCab blogcab = new BillDaeLogCab();
            blogcab.setTicket(response.getTicket().toString());
            blogcab.setCorrelativo(mensaje.getCorrelativo());
            blogcab.setFechaFin(new Date());
            blogcab.setFechaModificacion(new Date());
            blogcab.setUsuarioModificador(mensaje.getUsuario());

            BillDaeLogPack blogpack = new BillDaeLogPack();
            blogpack.setTicket(response.getTicket().toString());
            blogpack.setFechaFin(new Date());
            blogpack.setFechaModificacion(new Date());
            blogpack.setUsuarioModificador(mensaje.getUsuario());

            // si ocurre una excepcion se anula; por lo tanto se envia un mensaje al
            // buzon
            boolean envioBuzon = response.getCodeError() != 0 && response.isWithException();

            if (response.getCodeError() != 0 || mensaje.getFault() || envioBuzon) {
                blogpack.setEstadoProceso(99);
                blogcab.setEstadoProceso(99);
            } else {
                blogpack.setEstadoProceso(0);
                blogcab.setEstadoProceso(0);
            }
            blogcab.setCodigoResultado(response.getCodeError());
            updateDaeLogPackDAO.actualizaEstado(blogpack);
            
            if(response.getCodeError() == 100 || response.getCodeError() == 200 ) {
                BillDaeLogCab blogcabOld = updateDaeLogCabDAO.findByTicket(blogcab.getTicket(), mensaje.getCorrelativo());
                if(blogcabOld != null && blogcabOld.getEstadoProceso() == 98) {
                    updateDaeLogCabDAO.actualizaEstado(blogcab);
                }
                
            } else {
                updateDaeLogCabDAO.actualizaEstado(blogcab);
            }
        }else {
        	// 2.- Grabamos en las tablas del log
            BillLogCab blogcab = new BillLogCab();
            blogcab.setTicket(response.getTicket().toString());
            blogcab.setCorrelativo(mensaje.getCorrelativo());
            blogcab.setFechaFin(new Date());
            blogcab.setFechaModificacion(new Date());
            blogcab.setUsuarioModificador(mensaje.getUsuario());

            BillLogPack blogpack = new BillLogPack();
            blogpack.setTicket(response.getTicket().toString());
            blogpack.setFechaFin(new Date());
            blogpack.setFechaModificacion(new Date());
            blogpack.setUsuarioModificador(mensaje.getUsuario());

            // si ocurre una excepcion se anula; por lo tanto se envia un mensaje al
            // buzon
            boolean envioBuzon = response.getCodeError() != 0 && response.isWithException();

            if (response.getCodeError() != 0 || mensaje.getFault() || envioBuzon) {
                blogpack.setEstadoProceso(99);
                blogcab.setEstadoProceso(99);

            } else {
                blogpack.setEstadoProceso(0);
                blogcab.setEstadoProceso(0);

            }
            blogcab.setCodigoResultado(response.getCodeError());
            updateLogPackDAO.actualizaEstado(blogpack);
            
            if(response.getCodeError() == 100 || response.getCodeError() == 200 ) {
                BillLogCab blogcabOld = updateLogCabDAO.findByTicket(blogcab.getTicket(), blogcab.getCorrelativo());
                if(blogcabOld != null && blogcabOld.getEstadoProceso() == 98) {
                    updateLogCabDAO.actualizaEstado(blogcab);
                }
                
            } else {
                updateLogCabDAO.actualizaEstado(blogcab);
            }
            
            // PAS20211U210700014
//            if (blogpack.getEstadoProceso() == 0){
//            	if (log.isDebugEnabled()) log.debug("RespaldoServiceImpl.respaldarConstancia - EnvioKafkaGemAuditoria.actualizarMapCabecera");
//            	EnvioKafkaGemAuditoria.actualizarMapCabecera(blogpack);
//            	if (log.isDebugEnabled()) log.debug("RespaldoServiceImpl.respaldarConstancia - EnvioKafkaGemAuditoria.actualizarMapT4537logfecabIndividual");
//            	EnvioKafkaGemAuditoria.actualizarMapT4537logfecabIndividual(blogcab);
            	
//            	if (log.isDebugEnabled()) log.debug("RespaldoServiceImpl.respaldarConstancia - EnvioKafkaGemAuditoria.generarJsonAuditoria");
//            	EnvioKafkaGemAuditoria.generarJsonAuditoria();
            	
//            	if (log.isDebugEnabled()) log.debug("SaveFactura.insertComprobante - jsonkafka:" + EnvioKafkaGemAuditoria.json);
//    			if (log.isDebugEnabled()) log.debug("SaveFactura.insertComprobante - EnvioKafkaGemFactura.enviarJsonAuditoria");
//    			EnvioKafkaGemAuditoria.enviarJsonAuditoria();
//    			log.info("SaveFactura.insertComprobante - kafka enviado: " + EnvioKafkaGemAuditoria.kafkaTopicName + "; comprobante: " + EnvioKafkaGemAuditoria.compEnviado);
//            }
        }
        
        
        if (log.isDebugEnabled()) log.debug("RespaldoServiceImpl.respaldarConstancia - Fin");
    }

}
