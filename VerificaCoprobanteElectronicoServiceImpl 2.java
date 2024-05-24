package pe.gob.sunat.servicio2.registro.electronico.comppago.consulta.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.apache.xml.security.keys.KeyInfo;
import org.apache.xml.security.signature.XMLSignature;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import pe.gob.sunat.contribuyente.cpe.facturagem.model.BillStore;
import pe.gob.sunat.cpe.portal.cloud.Portal;
import pe.gob.sunat.cpe.portal.cloud.client.PortalClient;
import pe.gob.sunat.cpe.portal.cloud.client.URLConnectionPortalClient;
import pe.gob.sunat.cpe.portal.cloud.client.request.CpePortalClientRequest;
import pe.gob.sunat.cpe.portal.cloud.client.request.PortalClientRequest;
import pe.gob.sunat.cpe.portal.cloud.client.response.PortalQueryXmlFileResponse;
import pe.gob.sunat.cpe.portal.cloud.exception.AccessTokenException;
import pe.gob.sunat.cpe.portal.cloud.exception.PortalProblemException;
import pe.gob.sunat.cpe.portal.cloud.exception.PortalSystemException;
import pe.gob.sunat.facturaelectronica.portal.oauth.OAuthJSONAzureAccessToken;
import pe.gob.sunat.framework.spring.security.service.DigitalCertificateService;
import pe.gob.sunat.framework.spring.util.bean.MensajeBean;
import pe.gob.sunat.framework.spring.util.date.FechaBean;
import pe.gob.sunat.framework.spring.util.exception.ServiceException;
import pe.gob.sunat.servicio2.registro.electronico.compago.cloud.CloudComprobanteServiceImpl;
import pe.gob.sunat.servicio2.registro.electronico.compago.cloud.CloudComprobanteStatic;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.bean.ComprobanteParserBean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.context.FacturaContext;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.context.NotaCreditoContext;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.context.NotaDebitoContext;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T10194DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T10204DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T10209DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T4241DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T4243DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T4534DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T4536DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T4283DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6460CabGreDAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6464ArcGreDAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6571PercepcionSelectDAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6573RetencionSelectDAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6575ArchPerDAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6575DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6576ArchRetDAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.dao.T6576DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T10194Bean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T4241Bean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T4243Bean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T4534Bean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T4536Bean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T4283Bean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T6460CabGreBean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T6464ArcGreBean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T6571PercepcionBean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T6573RetencionBean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T6575ArchPerBean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.model.domain.T6576ArchRetBean;
import pe.gob.sunat.servicio2.registro.electronico.comppago.factura.service.ProcesaArchivoComprobanteService;
import pe.gob.sunat.servicio2.registro.electronico.comppago.model.dao.T3639DAO;
import pe.gob.sunat.servicio2.registro.electronico.comppago.model.domain.T3639Bean;
import pe.gob.sunat.servicio2.registro.model.dao.T01DAO;
import pe.gob.sunat.servicio2.registro.model.domain.T01Bean;
import pe.gob.sunat.servicio2.registro.model.util.UConstante;
import pe.gob.sunat.tecnologia2.validadoc.bean.VerifySignResult;
import sun.misc.BASE64Decoder;

/**
 * <p>
 * Title: VerificaCoprobanteElectronicoServiceImpl
 * </p>
 * <p>
 * Description: Implementación del Servicio de verificación del archivo digital.
 * </p>
 * 
 * @author jchuquitaype
 */
public class VerificaCoprobanteElectronicoServiceImpl implements VerificaCoprobanteElectronicoService {

	private static final Log log = LogFactory.getLog(VerificaCoprobanteElectronicoServiceImpl.class);

	private ProcesaArchivoComprobanteService archivoService;
	private DigitalCertificateService certificacionDigital;

	private T4241DAO t4241DAO;
	private T10194DAO t10194DAO;
	private T4243DAO t4243DAO;
	private T4243DAO t4243DAOSpring;
	private T3639DAO t3639DAO;

	private T4534DAO t4534DAO;

	private T6575ArchPerDAO t6575Dao;
	private T6571PercepcionSelectDAO t6571Dao;

	private T6576ArchRetDAO t6576Dao;
	private T6576DAO t6576HistDao;

	private T6575DAO t6575HistDao;

	private T6573RetencionSelectDAO t6573Dao;

	private T6464ArcGreDAO t6464Dao;
	private T6460CabGreDAO t6460Dao;

	private T4536DAO daoT4536Hist;

	private T4536DAO daoT4536;

	private T01DAO t01DAO;
	
	private T10204DAO t10204DAO;
	
	private T10209DAO t10209DAO;
	
	// PAS20211U210700133 
	private T4283DAO t4283DAO; 

	protected static int IND_RUBRO_CLOUD_GUID = 32001;

	public T01DAO getT01DAO() {
		return t01DAO;
	}

	public void setT01DAO(T01DAO t01dao) {
		t01DAO = t01dao;
	}
	
	public T4243DAO gett4243DAOSpring() {
		return t4243DAOSpring;
	}

	public void sett4243DAOSpring(T4243DAO t4243DAOSpring) {
		this.t4243DAOSpring = t4243DAOSpring;
	}

	// 1) Buscar el documento en el documento en la Base de Datos
	// 2) Calcular el resumen del documento
	// 3) Obtener el resumen de la llave publica
	// 4) Comparar las firmas, firma obtenida del documento VS firma del documento
	// de la BD.
	public Map<String, String> evaluarFacturaGEM(Document doc, String tipodocumento, ComprobanteParserBean parser,
			File fileXml, byte[] bytes) {
		if (log.isInfoEnabled())
			log.info(">>evaluarFacturaGEM");
		/**
		 * Key : mensaje Key : msgError
		 */
		Map<String, String> result = new HashMap<String, String>();
		String estadoDoc = "";

		try {
			
			
			Map<String, String> params = this.obtenerDatosComprobante(doc, parser.getXPath());
		
			/** Busca si el comprobante fue informado a SUNAT */
			T4243Bean t4243Bean = this.buscarArchivoGem(params.get("numRuc"), params.get("codCpe"),
					params.get("numSerieCpe"), new Integer(params.get("numCpe")), bytes);
			// T4243Bean t4243Bean =
			// t4243DAO.findFileXmlJoinTCabCPETArcXmlByPrimaryKey(params.get("numRuc"),
			// params.get("codCpe"), params.get("numSerieCpe"),
			// Integer.parseInt(params.get("numCpe")) );

			if (null == t4243Bean) {
				throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe") + "-"
						+ params.get("numCpe") + " no ha sido informado a SUNAT.");
			} else {
				// MSF: se evalua si el cpe esta activo
				T4241Bean t4241Bean = t4241DAO.findByPK(params.get("numRuc"), params.get("numSerieCpe"),
						new Integer(params.get("numCpe")), params.get("codCpe"));
				if (null != t4241Bean) {
					if (t4241Bean.getInd_estado().trim().equals("2")) {
						// throw new ServiceException(this,"El documento con número de serie "+
						// params.get("numSerieCpe")+"-"+params.get("numCpe")+" se encuentra de baja.");
						estadoDoc = "<br>Estado del Documento: Baja";
					} else {
						estadoDoc = "<br>Estado del Documento: Activo";
					}
				} else {
					throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe")
							+ "-" + params.get("numCpe") + " no ha sido informado a SUNAT.");
				}
			}
			/** Valida la firma electrónica. */
			this.validarFirma(doc, parser.getXPath(), fileXml);

			/** verificar contra documento reportado a SUNAT. */
			String queryXpath = "/sunat:" + doc.getDocumentElement().getNodeName()
					+ "/ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/ds:Signature/ds:SignatureValue";
			String signatureValueDoc = parser.getXPath().evaluate(queryXpath, doc, XPathConstants.STRING).toString();

			String signatureValueSUNAT = "";

			try {
				if (log.isDebugEnabled())
					log.debug(">> XML de SUNAT :: " + new String(decompress(t4243Bean.getArc_zip())));
				InputStream is = new ByteArrayInputStream(decompress(t4243Bean.getArc_zip()));
				ComprobanteParserBean parserBean = new ComprobanteParserBean();
				Document docSUNAT = parserBean.parse(is);
				String typeDoc = docSUNAT.getDocumentElement().getNodeName().trim();

				/*
				 * PAS20171U210300083 obviar prefijos iniciales
				 */
				if (!typeDoc.equals("Invoice") || !typeDoc.equals("CreditNote") || !typeDoc.equals("DebitNote")) {
					typeDoc = docSUNAT.getDocumentElement().getLocalName().trim();
				}

				if (log.isDebugEnabled())
					log.debug("evaluarFacturaGEM - doc.getDocumentElement().getNodeName().trim() = "
							+ docSUNAT.getDocumentElement().getNodeName().trim());
				if (log.isDebugEnabled())
					log.debug("evaluarFacturaGEM - doc.getDocumentElement().getLocalName().trim() = "
							+ docSUNAT.getDocumentElement().getLocalName().trim());

				if (typeDoc.equals("Invoice"))
					parserBean.setComprobanteContext(new FacturaContext());
				else if (typeDoc.equals("CreditNote"))
					parserBean.setComprobanteContext(new NotaCreditoContext());
				else if (typeDoc.equals("DebitNote"))
					parserBean.setComprobanteContext(new NotaDebitoContext());
				queryXpath = "/sunat:" + typeDoc
						+ "/ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/ds:Signature/ds:SignatureValue";
				signatureValueSUNAT = parserBean.getXPath().evaluate(queryXpath, docSUNAT, XPathConstants.STRING)
						.toString();

			} catch (Exception e) {
				log.error(e, e);
				throw new Exception("Ocurrio un error al procesar el documento.");
			}

			if (!signatureValueDoc.trim().equals(signatureValueSUNAT.trim()))
				throw new ServiceException(this,
						"El comprobante ingresado no es igual al comprobante informado a SUNAT.");

			result.put("mensaje",
					"El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. "
							+ estadoDoc);

		} catch (ServiceException e) {
			log.error(e, e);
			result.put("msgError", "Comprobante no valido. " + e.getMessage());
		} catch (Exception e) {
			log.error(e, e);
			e.printStackTrace();
			throw new ServiceException(this, "Ocurrio un error al realizar la verificación del documento.");
		}
		return result;
	}

	public Map<String, String> consultaGrabaFileNube(Document doc, String tipodocumento, ComprobanteParserBean parser, File fileXml, byte[] bytes) {
		if (log.isInfoEnabled()) log.info(">>consultaGrabaFileNube");
		Map<String, String> result = new HashMap<String, String>();
		String estadoDoc = "";
		Map<String, String> params = null;
		try {
			params = this.obtenerDatosComprobante(doc, parser.getXPath());
			T4243Bean t4243Bean_ = this.buscarArchivoNube(params.get("numRuc"), params.get("codCpe"),
					params.get("numSerieCpe"), new Integer(params.get("numCpe")), bytes);
			
			
			if (t4243Bean_ == null) {
				throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe") + "-"
						+ params.get("numCpe") + " no ha sido informado a SUNAT.");
			} 
			else 
			{			
				if (log.isInfoEnabled()) log.info(">>t4243Bean_.toString()=="+ t4243Bean_.toString());
				
				// MSF: se evalua si el cpe esta activo
				T4241Bean t4241Bean = null;
				
				if (params.get("codCpe").equals ( "30" ) || params.get("codCpe").equals ( "42" )) {
	    			t4241Bean = t10204DAO.findByPK ( params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe") );
	    		} else if (params.get("codCpe").equals ( "34" )) {
	    			t4241Bean = t10209DAO.findByPK ( params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe") );
	    		} else {
	    			t4241Bean = t4241DAO.findByPK(params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe"));
	    		}
				if (log.isInfoEnabled()) log.info(">>t4241Bean-" + t4241Bean.toString());
				if (null != t4241Bean) {
					if (log.isInfoEnabled()) log.info(">>t4241Bean.getInd_estado()-" + t4241Bean.getInd_estado().trim());
					if (t4241Bean.getInd_estado().trim().equals("2")) {
						// throw new ServiceException(this,"El documento con número de serie "+
						// params.get("numSerieCpe")+"-"+params.get("numCpe")+" se encuentra de baja.");
						estadoDoc = "<br>Estado del Documento: Baja";
					} else {
						estadoDoc = "<br>Estado del Documento: Activo";
					}
					if (log.isInfoEnabled()) log.info(">>estadoDoc-" + estadoDoc);
				} else {
					throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe")
							+ "-" + params.get("numCpe") + " no ha sido informado a SUNAT.");
				}
				if (log.isInfoEnabled()) log.info(">>fin-consultaGrabaFileNube");
				result.put("mensaje",
						"El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. "
								+ estadoDoc);
			}
		} catch (ServiceException e) {
			log.error(e, e);
			result.put("msgError", "Comprobante no valido. " + e.getMessage());
		} catch (Exception e) {
			log.error(e, e);
			e.printStackTrace();
			throw new ServiceException(this, "Ocurrio un error al realizar la verificación del documento.");
		}
		log.debug("result" + result);
		return result;
			
	}
	public Map<String, String> evaluarFileFacturaGEM(Document doc, String tipodocumento, ComprobanteParserBean parser, File fileXml, byte[] bytes) {
		if (log.isInfoEnabled())
			log.info(">>evaluarFileFacturaGEM");
		/**
		 * Key : mensaje Key : msgError
		 */
		Map<String, String> result = new HashMap<String, String>();
		String estadoDoc = "";
		Map<String, String> params = null;

		try {	
			params = this.obtenerDatosComprobante(doc, parser.getXPath());

			T4243Bean t4243Bean = this.buscarArchivoNube(params.get("numRuc"), params.get("codCpe"),
					params.get("numSerieCpe"), new Integer(params.get("numCpe")), bytes);
			
			if (null == t4243Bean) {
				throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe") + "-"
						+ params.get("numCpe") + " no ha sido informado a SUNAT.");
			} else {
				// MSF: se evalua si el cpe esta activo
				T4241Bean t4241Bean = null;
				
				if (params.get("codCpe").equals ( "30" ) || params.get("codCpe").equals ( "42" )) {
	    			t4241Bean = t10204DAO.findByPK ( params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe") );
	    		} else if (params.get("codCpe").equals ( "34" )) {
	    			t4241Bean = t10209DAO.findByPK ( params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe") );
	    		} else {
	    			t4241Bean = t4241DAO.findByPK(params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe"));
	    		}
				
				if (null != t4241Bean) {
					if (t4241Bean.getInd_estado().trim().equals("2")) {
						// throw new ServiceException(this,"El documento con número de serie "+
						// params.get("numSerieCpe")+"-"+params.get("numCpe")+" se encuentra de baja.");
						estadoDoc = "<br>Estado del Documento: Baja";
					} else {
						estadoDoc = "<br>Estado del Documento: Activo";
					}
				} else {
					throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe")
							+ "-" + params.get("numCpe") + " no ha sido informado a SUNAT.");
				}
			}
			/** Valida la firma electrónica. */
			try {
				log.info(">>validarFirma");
				this.validarFirma(doc, parser.getXPath(), fileXml);
				log.info(">>fin-validarFirma");	
			} catch (ServiceException se) {
				log.error(se, se);
				throw se;
			} catch (Exception e) {
				log.error(e, e);
				throw new ServiceException(this,
						"No se pudo validar la firma del documento, se presenta el error: " + e.getMessage());
			}
			
			/** verificar contra documento reportado a SUNAT. */
			String queryXpath = "/sunat:" + doc.getDocumentElement().getNodeName()
					+ "/ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/ds:Signature/ds:SignatureValue";
			String signatureValueDoc = parser.getXPath().evaluate(queryXpath, doc, XPathConstants.STRING).toString();

			String signatureValueSUNAT = "";

			try {
				T4243Bean t4243Bean_ = this.buscarArchivoNube(params.get("numRuc"), params.get("codCpe"),
						params.get("numSerieCpe"), new Integer(params.get("numCpe")), bytes);

				//log.debug(">> XML de SUNAT 1 :: " + new String(decompressZip(t4243Bean_.getArc_zip())));
				//log.debug(">> XML de SUNAT 2 :: " + new String(decompressZip(t4243Bean.getArc_zip())));

				if (!(new String(decompressZip(t4243Bean_.getArc_zip()))).equals("")) {

					log.info("Archivo bien formado");

					InputStream is = new ByteArrayInputStream(decompressZip(t4243Bean.getArc_zip()));
					ComprobanteParserBean parserBean = new ComprobanteParserBean();
					Document docSUNAT = parserBean.parse(is);
					String typeDoc = docSUNAT.getDocumentElement().getNodeName().trim();

					/*
					 * PAS20171U210300083 obviar prefijos iniciales
					 */
					if (!typeDoc.equals("Invoice") || !typeDoc.equals("CreditNote") || !typeDoc.equals("DebitNote")) {
						typeDoc = docSUNAT.getDocumentElement().getLocalName().trim();
					}

					if (log.isDebugEnabled())
						log.debug("evaluarFileFacturaGEM - doc.getDocumentElement().getNodeName().trim() = "
								+ docSUNAT.getDocumentElement().getNodeName().trim());
					if (log.isDebugEnabled())
						log.debug("evaluarFileFacturaGEM - doc.getDocumentElement().getLocalName().trim() = "
								+ docSUNAT.getDocumentElement().getLocalName().trim());

					if (typeDoc.equals("Invoice"))
						parserBean.setComprobanteContext(new FacturaContext());
					else if (typeDoc.equals("CreditNote"))
						parserBean.setComprobanteContext(new NotaCreditoContext());
					else if (typeDoc.equals("DebitNote"))
						parserBean.setComprobanteContext(new NotaDebitoContext());
					queryXpath = "/sunat:" + typeDoc
							+ "/ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/ds:Signature/ds:SignatureValue";
					signatureValueSUNAT = parserBean.getXPath().evaluate(queryXpath, docSUNAT, XPathConstants.STRING)
							.toString();

					if (!signatureValueDoc.trim().equals(signatureValueSUNAT.trim()))
						throw new ServiceException(this,
								"El comprobante ingresado no es igual al comprobante informado a SUNAT.");
				} else {
					// en caso haya problemas al descomprimir el zip
					// Actualizando nuevamente el xml
					log.info("Archivo mal formado, update el zip del xml");

					Map<String, String> MapTicketFestore = new HashMap<String, String>();
					// obteniendo num_ticket
					MapTicketFestore = t4243DAO.find_ticket_CPE(params.get("numRuc"), params.get("codCpe"),
							params.get("numSerieCpe"), new Integer(params.get("numCpe")));

					String correlativo = String.valueOf(MapTicketFestore.get("num_correl"));
					String arrayZip = "";

					BillStore BillStore_ = new BillStore();
					BillStore_.setTicket(MapTicketFestore.get("num_ticket"));
					BillStore_.setModo("1");
				//	BillStore_.setContenido(zipBytes(arrayZip, bytes));
					BillStore_.setContenido(bytes);
					BillStore_.setCorrelativo(Integer.valueOf(correlativo));

					t4243DAOSpring.update_ticket_CPE(BillStore_);

				}

			} catch (Exception e) {
				log.error(e, e);
				throw new Exception("Ocurrio un error al procesar el documento.");
			}

			result.put("mensaje",
					"El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. "
							+ estadoDoc);

		} catch (ServiceException e) {
			log.error(e, e);
			result.put("msgError", "Comprobante no valido. " + e.getMessage());
		} catch (Exception e) {
			log.error(e, e);
			e.printStackTrace();
			throw new ServiceException(this, "Ocurrio un error al realizar la verificación del documento.");
		}
		log.debug("result" + result);
		return result;
	}

	public Map<String, String> evaluarInputStream(Document doc, String tipodocumento, ComprobanteParserBean parser,byte[] bytes) throws ServiceException, Exception {
		String respuesta = "Error.";
		String estadoDoc_ = "";
		Map<String, String> mensaje = new HashMap<String, String>();

		if (log.isDebugEnabled()) log.debug("Inicio - Comparación archivo con archivo");

		try {
			// obtiene datos del xml ingresado
			Map<String, String> params = this.obtenerDatosComprobante(doc, parser.getXPath());
			String numRuc = params.get("numRuc");
			String serie = params.get("numSerieCpe");
			Integer numCpe = new Integer(params.get("numCpe"));
			String codCpe = params.get("codCpe");

			T4243Bean t4243Bean = new T4243Bean();
			t4243Bean.setNum_ruc(numRuc);
			t4243Bean.setNum_serie_cpe(serie);
			t4243Bean.setNum_cpe(numCpe);
			t4243Bean.setCod_cpe(codCpe);
			//inicio bloque1
			String textoJson="";
			String res ="";
			String nomArchivo="";
			
			
			
			
			if (tipodocumento.equals("Invoice") || tipodocumento.equals("DebitNote") || tipodocumento.equals("CreditNote")) {
			if (tipodocumento.equals("InvoiceGEM") || tipodocumento.equals("DebitNoteGEM") || tipodocumento.equals("CreditNoteGEM")) {
				T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
				FechaBean fb = t4241Bean.getFec_emision();
				Date fechaEmi = fb.getTimestamp();
			if (tipodocumento.equals("PerceptionGEM")) {
				T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
				FechaBean fb = t4241Bean.getFec_emision();
				Date fechaEmi = fb.getTimestamp();
			if (tipodocumento.equals("RetentionGEM")) {
				T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
				FechaBean fb = t4241Bean.getFec_emision();
				Date fechaEmi = fb.getTimestamp();
			if (tipodocumento.equals("DespatchAdviceGEM")) {
				T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
				FechaBean fb = t4241Bean.getFec_emision();
				Date fechaEmi = fb.getTimestamp();
			
			
			
			
			
			
			try {
				T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
				FechaBean fb = t4241Bean.getFec_emision();
				Date fechaEmi = fb.getTimestamp();
			
				if(log.isDebugEnabled()) log.debug("fechaEmi"+fechaEmi);
			
				Calendar calendario = Calendar.getInstance();
				calendario.setTime(fechaEmi);
					
				String annio = String.valueOf(calendario.get(Calendar.YEAR));
				String mes = String.format("%02d", calendario.get(Calendar.MONTH) + 1);
				String dia = String.format("%02d", calendario.get(Calendar.DAY_OF_MONTH));
				String hora = String.format("%02d", calendario.get(Calendar.HOUR_OF_DAY));
	        
				if(log.isDebugEnabled()) log.debug("annio: "+annio+" - mes: "+mes+" - dia: "+dia+" - hora: "+hora);
				
				char ultimoDigitoRuc = numRuc.charAt(numRuc.length() - 1);
				String ruta = "no encontro ruta";
				
				ruta= annio +"/"+mes+"/"+dia+"/"+hora+"/"+ultimoDigitoRuc+"/"+codCpe+"/0";
				if (log.isDebugEnabled()) log.debug("mostrar ruta: " + ruta);
				
				nomArchivo=numRuc + "-"+ codCpe + "-" + serie + "-" + numCpe + ".zip";
				textoJson = "{\"nomArchivo\":\"" + nomArchivo + "\",\"ruta\":\""+ruta+"\"}";
				
				if (log.isDebugEnabled()) log.debug("mostrar json: " + textoJson);
				
			} catch (Exception e) {
				if (log.isDebugEnabled()) log.debug("Error: "+e);
			}
			//fin bloque1			

			if (log.isDebugEnabled()) log.debug("DVDR - VerificaCoprobanteElectronicoServiceImpl.evaluarInputStream : " + numRuc + "-"+ codCpe + "-" + serie + "-" + numCpe);

			if (tipodocumento.equals("Invoice") || tipodocumento.equals("DebitNote") || tipodocumento.equals("CreditNote")) {
				
				if (log.isDebugEnabled()) log.debug("DVDR - Invoice/DebitNote/CreditNote - Inicio");
				
				
					//inicio bloque1
				String textoJson="";
				String res ="";
				String nomArchivo="";
				try {
					T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
					FechaBean fb = t4241Bean.getFec_emision();
					Date fechaEmi = fb.getTimestamp();
				
					if(log.isDebugEnabled()) log.debug("fechaEmi"+fechaEmi);
				
					Calendar calendario = Calendar.getInstance();
					calendario.setTime(fechaEmi);
						
					String annio = String.valueOf(calendario.get(Calendar.YEAR));
					String mes = String.format("%02d", calendario.get(Calendar.MONTH) + 1);
					String dia = String.format("%02d", calendario.get(Calendar.DAY_OF_MONTH));
					String hora = String.format("%02d", calendario.get(Calendar.HOUR_OF_DAY));
				
					if(log.isDebugEnabled()) log.debug("annio: "+annio+" - mes: "+mes+" - dia: "+dia+" - hora: "+hora);
					
					char ultimoDigitoRuc = numRuc.charAt(numRuc.length() - 1);
					String ruta = "no encontro ruta";
					
					ruta= annio +"/"+mes+"/"+dia+"/"+hora+"/"+ultimoDigitoRuc+"/"+codCpe+"/0";
					if (log.isDebugEnabled()) log.debug("mostrar ruta: " + ruta);
					
					nomArchivo=numRuc + "-"+ codCpe + "-" + serie + "-" + numCpe + ".zip";
					textoJson = "{\"nomArchivo\":\"" + nomArchivo + "\",\"ruta\":\""+ruta+"\"}";
					
					if (log.isDebugEnabled()) log.debug("mostrar json: " + textoJson);
					
				} catch (Exception e) {
					if (log.isDebugEnabled()) log.debug("Error: "+e);
				}
				//fin bloque1
				
				
				
				
				
			//inicio bloque2	
				if (log.isDebugEnabled()) log.debug("DVDR - Invoice/DebitNote/CreditNote - Inicio - consultarNFS");
				res = buscarArchivoNFS(textoJson);
				if (log.isDebugEnabled()) log.debug("consultarNFS res: "+res);
				
				if (res != null) {
					t4243Bean.setArc_xml(this.buscarArchivoNFS(textoJson));
					if (log.isDebugEnabled()) log.debug("DVDR - Invoice/DebitNote/CreditNote - setArc_xml - textoJson: "+textoJson);
					//comprimir el base 64
					t4243Bean.setArc_zip(zipBytes(nomArchivo, bytes));
					if (log.isDebugEnabled()) log.debug("DVDR - Invoice/DebitNote/CreditNote - setArc_zip - nomArchivo: "+nomArchivo);
					if (log.isDebugEnabled()) log.debug("DVDR - Invoice/DebitNote/CreditNote - Fin - consultarNFS");
			//fin bloque2
				} else {
				t4243Bean = t4243DAO.findByRUC_Serie_CPE(t4243Bean);
				}
				if (null == t4243Bean) {
					//respuesta = "NO INFORMADO";
					throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe") + "-" + params.get("numCpe") + " no ha sido informado a SUNAT.");
				} else {
					
					InputStream stream_utf8 = new ByteArrayInputStream(t4243Bean.getArc_xml().getBytes("UTF-8"));
					InputStream stream = new ByteArrayInputStream(t4243Bean.getArc_xml().getBytes());
					
					byte[] bytes2_utf8 = IOUtils.toByteArray(stream_utf8);
					byte[] bytes2 = IOUtils.toByteArray(stream);
					
					//limpiando caracteres extraños al inicio
					InputStream stream_ = new ByteArrayInputStream(new String(bytes).replaceFirst("^([\\W]+)<", "<").getBytes());
					byte[] bytes_ = IOUtils.toByteArray(stream_);
					
					//limpiando salto de linea (para archivos en formatos unix, windows, mac)
					InputStream stream_input = new ByteArrayInputStream(new String(bytes).replaceFirst("^([\\W]+)<", "<").replaceAll("[\n\r]", "").getBytes());
					byte[] bytes_input = IOUtils.toByteArray(stream_input);				
					//InputStream stream2_sunat = new ByteArrayInputStream(new String(bytes2).replaceAll("[\n\r]", "").getBytes());
					InputStream stream2_sunat = new ByteArrayInputStream(new String(bytes2).replaceFirst("^([\\W]+)<", "<").replaceAll("[\n\r]", "").getBytes());
					byte[] bytes2_sunat = IOUtils.toByteArray(stream2_sunat);
					

					if ( Arrays.equals(bytes_, bytes2) || Arrays.equals(bytes_, bytes2_utf8) || Arrays.equals(bytes_input, bytes2_sunat) ) {
						if (existeCdpActivo("Invoice", doc, parser)) {
							estadoDoc_ = "<br>Estado del Documento: Activo";
						} else {
							estadoDoc_ = "<br>Estado del Documento: Baja";
						}
						respuesta = "El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. " + estadoDoc_;
					} else {
						respuesta = "DIFERENTES";
					}					
				}
				if (log.isDebugEnabled()) log.debug("DVDR - Invoice/DebitNote/CreditNote - Fin");

			} else if (tipodocumento.equals("InvoiceGEM") || tipodocumento.equals("DebitNoteGEM") || tipodocumento.equals("CreditNoteGEM")) {

				if (log.isDebugEnabled()) log.debug("DVDR - InvoiceGEM/DebitNoteGEM/CreditNoteGEM - Inicio");
				//inicio bloque3	
				if (log.isDebugEnabled()) log.debug("DVDR - InvoiceGEM/DebitNoteGEM/CreditNoteGEM - Inicio - consultarNFS");
				res = buscarArchivoNFS(textoJson);
				if (log.isDebugEnabled()) log.debug("consultarNFS res: "+res);
				
				if (res != null) {
					t4243Bean.setArc_xml(this.buscarArchivoNFS(textoJson));
					//comprimir el base 64
					t4243Bean.setArc_zip(zipBytes(nomArchivo, bytes));
					if (log.isDebugEnabled()) log.debug("DVDR - InvoiceGEM/DebitNoteGEM/CreditNoteGEM - Fin - consultarNFS");
				//fin bloque3
				} else {							
				t4243Bean = this.buscarArchivoGemConGrabado(numRuc, codCpe, serie, numCpe, doc, parser, bytes);
				}
				if (log.isDebugEnabled()) log.debug("buscarArchivoGemConGrabado devuelve t4243Bean de nube");
				if (null == t4243Bean) {
					//respuesta = "NO INFORMADO";
					throw new ServiceException(this, "El documento con número de serie " + params.get("numSerieCpe") + "-" + params.get("numCpe") + " no ha sido informado a SUNAT.");
				} else {
					
					InputStream stream = null;
					
					if(t4243Bean.getArc_zip() != null){
		    			if(log.isDebugEnabled()) log.debug("==xmlZip==1");
		    		}
					if(t4243Bean.getArc_xml() != null){
		    			if(log.isDebugEnabled()) log.debug("==xmlCloud==1");
		    		}
					
					stream = new ByteArrayInputStream(decompress(t4243Bean.getArc_zip()));
					
		    		/*if(t4243Bean.getArc_zip() != null){
		    			if(log.isDebugEnabled()) log.debug("==xmlZip==");
		    			stream = new ByteArrayInputStream(decompress(t4243Bean.getArc_zip()));
		    		}else if(t4243Bean.getArc_xml() != null){
		    			if(log.isDebugEnabled()) log.debug("==xmlCloud==");
		    			stream = new ByteArrayInputStream(t4243Bean.getArc_xml().getBytes("UTF-8"));
		    		}*/
					
					//InputStream stream = new ByteArrayInputStream(decompress(t4243Bean.getArc_zip()));
					byte[] bytes2 = IOUtils.toByteArray(stream);

					//limpiando caracteres extraños al inicio del documento ingresado
					InputStream stream_ = new ByteArrayInputStream(new String(bytes).replaceFirst("^([\\W]+)<", "<").getBytes());
					byte[] bytes_ = IOUtils.toByteArray(stream_);
					
					//limpiando salto de linea (para archivos en formatos unix, windows, mac)
					InputStream stream_input = new ByteArrayInputStream(new String(bytes).replaceFirst("^([\\W]+)<", "<").replaceAll("[\n\r]", "").getBytes());
					byte[] bytes_input = IOUtils.toByteArray(stream_input);				
					//InputStream stream2_sunat = new ByteArrayInputStream(new String(bytes2).replaceAll("[\n\r]", "").getBytes());
					InputStream stream2_sunat = new ByteArrayInputStream(new String(bytes2).replaceFirst("^([\\W]+)<", "<").replaceAll("[\n\r]", "").getBytes());
					byte[] bytes2_sunat = IOUtils.toByteArray(stream2_sunat);
					
					
			
					
					if(log.isDebugEnabled())  log.debug("Arrays.equals(bytes_, bytes2)==" + Arrays.equals(bytes_, bytes2));
					if(log.isDebugEnabled())  log.debug("Arrays.equals(bytes_input, bytes2_sunat)==" + Arrays.equals(bytes_input, bytes2_sunat));
									
					if ( Arrays.equals(bytes_, bytes2) || Arrays.equals(bytes_input, bytes2_sunat) ){

						T4241Bean t4241Bean = null;
						
						String codCPE = params.get("codCpe") != null ? params.get("codCpe") : "";
						if (codCPE.equals ( "30" ) || codCPE.equals ( "42" )) {
			    			t4241Bean = t10204DAO.findByPK ( params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe") );
			    		} else if (codCPE.equals ( "34" )) {
			    			t4241Bean = t10209DAO.findByPK ( params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe") );
			    		} else {
			    			t4241Bean = t4241DAO.findByPK(params.get("numRuc"), params.get("numSerieCpe"), new Integer(params.get("numCpe")), params.get("codCpe"));
			    		}
						
						if (null != t4241Bean) {
							if (t4241Bean.getInd_estado().trim().equals("2")) {
								estadoDoc_ = "<br>Estado del Documento: Baja";
							} else {
								estadoDoc_ = "<br>Estado del Documento: Activo";
							}							
						} else {
							throw new ServiceException(this,"El documento con número de serie " + params.get("numSerieCpe") + "-" + params.get("numCpe") + " no ha sido informado a SUNAT.");
						}
						respuesta = "El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. " + estadoDoc_;
					} else {
						respuesta = "DIFERENTES";
					}

				}

				if (log.isDebugEnabled()) log.debug("DVDR - InvoiceGEM/DebitNoteGEM/CreditNoteGEM - Fin");

			} else if (tipodocumento.equals("PerceptionGEM")) {

				if (log.isDebugEnabled()) log.debug("DVDR - PerceptionGEM - Inicio");
				//inicio bloque4
				T6575ArchPerBean t6575 = new T6575ArchPerBean();
				if (log.isDebugEnabled()) log.debug("DVDR - PerceptionGEM - Inicio - consultarNFS");
				res = buscarArchivoNFS(textoJson);
				if (log.isDebugEnabled()) log.debug("consultarNFS res: "+res);
				
				if (res != null) {
					t6575.setArc_xml(this.buscarArchivoNFS(textoJson));
					//comprimir el base 64
					t6575.setArcArchivo(zipBytes(nomArchivo, bytes));
					if (log.isDebugEnabled()) log.debug("DVDR - PerceptionGEM - Fin - consultarNFS");
				//fin bloque4
				} else {
//				T6575ArchPerBean t6575 = this.buscarArchivoPerceptionGEM(numRuc, codCpe, serie, numCpe);
				t6575 = this.buscarArchivoPerceptionGEM(numRuc, codCpe, serie, numCpe);
				}	
				if ( null == t6575 ) {
					//respuesta = "NO INFORMADO";
					throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe + " no ha sido informado a SUNAT.");
				} else {

		    		InputStream stream = null;
		    		
		    		if( t6575.getArcArchivo() != null ){
		    			if(log.isDebugEnabled()) log.debug("==xmlZip==2");
		    			stream = new ByteArrayInputStream(decompress(t6575.getArcArchivo()));	    			
		    		}else if(t6575.getArc_xml() != null){
		    			if(log.isDebugEnabled()) log.debug("==xmlCloud==2");
		    			stream = new ByteArrayInputStream(t6575.getArc_xml().getBytes("UTF-8"));	    			
		    		}
		    		
					//InputStream stream = new ByteArrayInputStream(decompress(t6575.getArcArchivo()));
					byte[] bytes2 = IOUtils.toByteArray(stream);
					

					if(log.isDebugEnabled())  log.debug("Arrays.equals(bytes_, bytes2)==" + Arrays.equals(bytes, bytes2));
			
					if (Arrays.equals(bytes, bytes2)) {
						
						T6571PercepcionBean t4571Bean = t6571Dao.buscarPorPk(numRuc, codCpe, serie, numCpe);
						if (null != t4571Bean) {
							if (t4571Bean.getCodEstadoCpe().trim().equals("3") || t4571Bean.getCodEstadoCpe().trim().equals("03")) {
								estadoDoc_ = "<br>Estado del Documento: Anulado";
							} else if (t4571Bean.getCodEstadoCpe().trim().equals("2") || t4571Bean.getCodEstadoCpe().trim().equals("02")) {
								estadoDoc_ = "<br>Estado del Documento: Revertido";
							} else {
								estadoDoc_ = "<br>Estado del Documento: Activo";
							}							
						} else {
							throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe + " no ha sido informado a SUNAT.");
						}
						respuesta = "El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. "  + estadoDoc_;
						
					} else {
						respuesta = "DIFERENTES";
					}

				}
				if (log.isDebugEnabled()) log.debug("DVDR - PerceptionGEM - Fin");

			} else if (tipodocumento.equals("RetentionGEM")) {

				if (log.isDebugEnabled()) log.debug("DVDR - RetentionGEM - Inicio");
				//inicio bloque5
				T6576ArchRetBean t6576 = new T6576ArchRetBean ();
				if (log.isDebugEnabled()) log.debug("DVDR - RetentionGEM - Inicio - consultarNFS");
				res = buscarArchivoNFS(textoJson);
				if (log.isDebugEnabled()) log.debug("consultarNFS res: "+res);
				
				if (res != null) {
					t6576.setArc_xml(this.buscarArchivoNFS(textoJson));
					//comprimir el base 64
					t6576.setArcArchivo(zipBytes(nomArchivo, bytes));
					if (log.isDebugEnabled()) log.debug("DVDR - RetentionGEM - Fin - consultarNFS");
				//fin bloque5
				} else {
//				T6576ArchRetBean t6576 = this.buscarArchivoRetentionGEM(numRuc, codCpe, serie, numCpe);
				t6576 = this.buscarArchivoRetentionGEM(numRuc, codCpe, serie, numCpe);
				}
				if (null == t6576) {
					//respuesta = "NO INFORMADO";
					throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe + " no ha sido informado a SUNAT.");
				} else {

					InputStream stream = null;
		    		
		    		if( t6576.getArcArchivo() != null ){
		    			if(log.isDebugEnabled()) log.debug("==xmlZip==3");
		    			stream = new ByteArrayInputStream(decompress(t6576.getArcArchivo()));	    			
		    		}else if(t6576.getArc_xml() != null){
		    			if(log.isDebugEnabled()) log.debug("==xmlCloud==3");
		    			stream = new ByteArrayInputStream(t6576.getArc_xml().getBytes("UTF-8"));
		    		}
		    		
					//InputStream stream = new ByteArrayInputStream(decompress(t6576.getArcArchivo()));
					byte[] bytes2 = IOUtils.toByteArray(stream);		

					if(log.isDebugEnabled())  log.debug("Arrays.equals(bytes_, bytes2)==" + Arrays.equals(bytes, bytes2));
					if (Arrays.equals(bytes, bytes2)) {
						
						T6573RetencionBean t4573Bean = t6573Dao.buscarPorPk(numRuc, codCpe, serie, numCpe);
						if (null != t4573Bean) {
							if (t4573Bean.getCodEstCpe().trim().equals("3") || t4573Bean.getCodEstCpe().trim().equals("03")) {
								estadoDoc_ = "<br>Estado del Documento: Anulado";
							} else if (t4573Bean.getCodEstCpe().trim().equals("2") || t4573Bean.getCodEstCpe().trim().equals("02")) {
								estadoDoc_ = "<br>Estado del Documento: Revertido";
							} else {
								estadoDoc_ = "<br>Estado del Documento: Activo";
							}
							
						} else {
							throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe + " no ha sido informado a SUNAT.");
						}

						respuesta = "El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. " + estadoDoc_;
						
					} else {
						respuesta = "DIFERENTES";
					}

				}

				if (log.isDebugEnabled()) log.debug("DVDR - RetentionGEM - Fin");

			} else if (tipodocumento.equals("DespatchAdviceGEM")) {

				if (log.isDebugEnabled()) log.debug("DVDR - DespatchAdviceGEM - Inicio");
				//inicio bloque6
				T6464ArcGreBean t6464 = new T6464ArcGreBean();
				if (log.isDebugEnabled()) log.debug("DVDR - DespatchAdviceGEM - Inicio - consultarNFS");
				res = buscarArchivoNFS(textoJson);
				if (log.isDebugEnabled()) log.debug("consultarNFS res: "+res);
				
				if (res != null) {
					t6464.setArc_xml(this.buscarArchivoNFS(textoJson));
					//comprimir el base 64
					t6464.setArcInput(zipBytes(nomArchivo, bytes));
					if (log.isDebugEnabled()) log.debug("DVDR - DespatchAdviceGEM - Fin - consultarNFS");
				//fin bloque6
				} else {
//				T6464ArcGreBean t6464 = this.buscarArchivoDespatchAdviceGEM(numRuc, codCpe, serie, numCpe);	
				t6464 = this.buscarArchivoDespatchAdviceGEM(numRuc, codCpe, serie, numCpe);
				}
				if (null == t6464) {
					//respuesta = "NO INFORMADO";
					throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe + " no ha sido informado a SUNAT.");
				} else {

		    		InputStream stream = null;
		    		
		    		if( t6464.getArcInput() != null ){
		    			if(log.isDebugEnabled()) log.debug("==xmlZip==4");
		    			stream = new ByteArrayInputStream(decompress(t6464.getArcInput()));	    			
		    		}else if(t6464.getArc_xml() != null){
		    			if(log.isDebugEnabled()) log.debug("==xmlCloud==4");
		    			stream = new ByteArrayInputStream(t6464.getArc_xml().getBytes("UTF-8"));
		    		}
		    		
					//InputStream stream = new ByteArrayInputStream(decompress(t6464.getArcInput()));
					byte[] bytes2 = IOUtils.toByteArray(stream);

					if(log.isDebugEnabled())  log.debug("Arrays.equals(bytes_, bytes2)==" + Arrays.equals(bytes, bytes2));
					if (Arrays.equals(bytes, bytes2)) {
						T6460CabGreBean t6460Bean = t6460Dao.buscarPorPk(numRuc, codCpe, serie, numCpe);
						if (null != t6460Bean) {
							if (t6460Bean.getIndEstado().trim().equals("0")) {
								estadoDoc_ = "<br>Estado del Documento: Activo";
							} else {
								estadoDoc_ = "<br>Estado del Documento: Baja";
							}
						} else {
							throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe + " no ha sido informado a SUNAT.");
						}

						respuesta = "El Documento Electrónico ingresado es íntegro, auténtico y cumple con el estándar establecido. " + estadoDoc_;

					} else {
						respuesta = "DIFERENTES";
					}

				}

				if (log.isDebugEnabled()) log.debug("DVDR - DespatchAdviceGEM - Fin");
			}

			if (log.isDebugEnabled()) log.debug("Respuesta - Comparación archivo con archivo::" + respuesta);

			if (log.isDebugEnabled()) log.debug("Fin - Comparación archivo con archivo");

		} catch (ServiceException e) {
			log.debug(e, e);
			mensaje.put("msgError", "Comprobante no valido. " + e.getMessage());
		}
		mensaje.put("mensaje", respuesta);
		log.debug("mensaje:" + mensaje.get("mensaje"));
		return mensaje;
	}

	public byte[] decompress(byte[] data) {

		try {

			InputStream theFile = new ByteArrayInputStream(data);
			ZipInputStream stream = new ZipInputStream(theFile);
			byte[] buffer = new byte[2048];
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

			ZipEntry entry;
			while ((entry = stream.getNextEntry()) != null) {
				String s = String.format("Entry: %s len %d added %TD", entry.getName(), entry.getSize(),
						new Date(entry.getTime()));
				if (log.isDebugEnabled())
					log.debug(s);
				/*
				 * En el caso del OSE ademas del XML envia su CDR, por ello debemos extraer solo el CPE
				 */
				if (entry.getName().indexOf("R-") == -1) {

					if (log.isDebugEnabled())
						log.debug("Ingresa el xml");

					try {
						outputStream = new ByteArrayOutputStream();
						int len = 0;

						while ((len = stream.read(buffer)) > 0) {
							outputStream.write(buffer, 0, len);
						}
					} finally {
						if (outputStream != null)
							outputStream.close();
					}
				}
			}

			byte[] output = outputStream.toByteArray();

			if (log.isDebugEnabled())
				log.debug("Archivo output:" + new String(output));

			return output;
		} catch (IOException e) {
			log.error(">>ConsultarServiceImpl Error !! " + e.getMessage(), e);
			MensajeBean msg = new MensajeBean();
			msg.setError(true);
			msg.setMensajeerror("Error al tratar de descomprimir el archivo ZIP");
			throw new ServiceException(this, msg);
		}

	}

	public static byte[] zipBytes(String filename, byte[] input) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ZipOutputStream zos = new ZipOutputStream(baos);
		ZipEntry entry = new ZipEntry(filename);
		entry.setSize(input.length);
		zos.putNextEntry(entry);
		zos.write(input);
		zos.closeEntry();
		zos.close();
		return baos.toByteArray();
	}

	public byte[] decompressZip(byte[] data) {

		try {

			InputStream theFile = new ByteArrayInputStream(data);
			ZipInputStream stream = new ZipInputStream(theFile);
			byte[] buffer = new byte[2048];
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

			ZipEntry entry;
			while ((entry = stream.getNextEntry()) != null) {
				String s = String.format("Entry: %s len %d added %TD", entry.getName(), entry.getSize(),
						new Date(entry.getTime()));
				if (log.isDebugEnabled())
					log.debug(s);

				try {
					outputStream = new ByteArrayOutputStream();
					;
					int len = 0;
					while ((len = stream.read(buffer)) > 0) {
						outputStream.write(buffer, 0, len);
					}
				} finally {
					if (outputStream != null)
						outputStream.close();
				}
			}
			byte[] output = outputStream.toByteArray();
			return output;
		} catch (IOException e) {
			log.error(">>ConsultarServiceImpl Error !! " + e.getMessage(), e);
			return null;
		}

	}

	/**
	 * Valida la firma del documento.
	 * 
	 * @param doc
	 * @param fileXml
	 */
	@SuppressWarnings("deprecation")
	private void validarFirma(Document doc, XPath xpath, File fileXml) {
		org.apache.xml.security.Init.init();
		try {
			log.debug("DVDR - validarFirma - 1");
			String queryXpath = "/sunat:" + doc.getDocumentElement().getNodeName()
					+ "/ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/ds:Signature";
			log.debug("DVDR - validarFirma - 2");
			NodeList signatureValueDoc = (NodeList) xpath.evaluate(queryXpath, doc, XPathConstants.NODESET);
			signatureValueDoc.item(0);
			log.debug("DVDR - validarFirma - 3");
			Element sigElement = (Element) signatureValueDoc.item(0);
			log.debug("DVDR - validarFirma - 4");
			log.debug("DVDR - validarFirma - 4 : sigElement " + sigElement);
			log.debug("DVDR - validarFirma - 4 : fileXml " + fileXml);
			log.debug("DVDR - validarFirma - 4 : fileXml.toURL() " + fileXml.toURL());
			XMLSignature signature = new XMLSignature(sigElement, fileXml.toURL().toString());
			log.debug("DVDR - validarFirma - 5");

			KeyInfo keyInfo = signature.getKeyInfo();
			if (keyInfo != null) {
				X509Certificate cert = keyInfo.getX509Certificate();
				if (cert != null) {
					if (!signature.checkSignatureValue(cert))
						throw new ServiceException(this, "El documento ha sido alterado. ");
				} else {
					PublicKey pk = keyInfo.getPublicKey();
					if (pk != null) {
						if (!signature.checkSignatureValue(pk))
							throw new ServiceException(this, "El documento ha sido alterado. ");
					} else {
						throw new ServiceException(this, "El documento no tiene la información del certificado. ");
					}
				}
			} else {
				throw new ServiceException(this, "El documento no tiene la información del certificado. ");
			}
			if (log.isDebugEnabled())
				log.debug(">> Paso la validación de la firma del documento.");
		} catch (ServiceException se) {
			log.error(se, se);
			throw se;
		} catch (Exception e) {
			log.error(e, e);
			throw new ServiceException(this,
					"No se pudo validar la firma del documento, se presenta el error: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param doc
	 * @param xpath
	 * @return Map con claves :
	 *         <ul>
	 *         <li>numRuc
	 *         <li>codCpe
	 *         <li>numSerieCpe
	 *         <li>numCpe
	 *         </ul>
	 */
	private Map<String, String> obtenerDatosComprobante(Document doc, XPath xpath) {
		boolean esGrebf = false;
		Map<String, String> result = new HashMap<String, String>();

		if (log.isDebugEnabled())
			log.debug("DVDR - obtenerDatosComprobante - Inic");

		try {
			String codCpe = "";
			String typeDoc = doc.getDocumentElement().getNodeName().trim();

			// PAS20171U210300083
			if (!typeDoc.equals("Invoice") || typeDoc.equals("CreditNote") || typeDoc.equals("DebitNote")
					|| !typeDoc.equals("Perception") || typeDoc.equals("Retention")
					|| typeDoc.equals("DespatchAdvice")) {
				typeDoc = doc.getDocumentElement().getLocalName().trim();
			}

			if (log.isDebugEnabled())
				log.debug("DVDR - doc.getDocumentElement().getNodeName().trim() = "
						+ doc.getDocumentElement().getNodeName().trim());
			if (log.isDebugEnabled())
				log.debug("DVDR - doc.getDocumentElement().getLocalName().trim() = "
						+ doc.getDocumentElement().getLocalName().trim());

			if (typeDoc.equals("Invoice")) {
				codCpe = "01";
			} else if (typeDoc.equals("CreditNote")) {
				codCpe = "07";
			} else if (typeDoc.equals("DebitNote")) {
				codCpe = "08";
			} else if (typeDoc.equals("Perception")) {
				codCpe = "40";
			} else if (typeDoc.equals("Retention")) {
				codCpe = "20";
			} else if (typeDoc.equals("DespatchAdvice")) { // MSF: Se incluye validacion para gre-bf
				esGrebf = true;
				String greXpath = "/sunat:" + typeDoc + "/cbc:DespatchAdviceTypeCode";
				Object tipoGre = xpath.evaluate(greXpath, doc, XPathConstants.STRING);
				if (null == tipoGre) {
					throw new ServiceException(this, "El documento no tiene el tipo de guía de remisión.");
				} else {
					codCpe = tipoGre.toString().trim();
				}
			}

			String queryXpath = "";			
			
			//log.debug("Version de UBL 1:" + doc.getElementsByTagName("cbc:UBLVersionID") );
			//log.debug("Version de UBL 2:" + doc.getElementsByTagName("cbc:UBLVersionID").item(0) );
			//log.debug("Version de UBL 3:" + doc.getElementsByTagName("cbc:UBLVersionID").item(0).getTextContent() );			
			
			String ublVersion = "";
			
			if(doc.getElementsByTagName("cbc:UBLVersionID").item(0) != null){
				log.debug("Version de UBL cbc:UBLVersionID");
				ublVersion = doc.getElementsByTagName("cbc:UBLVersionID").item(0).getTextContent(); 
			}else{
				log.debug("Version de UBL :UBLVersionID");
				ublVersion = obtenerContenidoNodo(doc.getDocumentElement(), ":UBLVersionID");
			}
			
			//if(ublVersion == null){
			//	ublVersion = obtenerContenidoNodo(doc.getDocumentElement(), ":UBLVersionID");
			//}
			log.debug("Version de UBL :" + ublVersion );
			
			if (esGrebf) {
				queryXpath = "/sunat:" + typeDoc + "/cac:DespatchSupplierParty/cbc:CustomerAssignedAccountID";
			} else {
				if (typeDoc.equals("Perception") || typeDoc.equals("Retention")) {
					queryXpath = "/sunat:" + typeDoc + "/cac:AgentParty/cac:PartyIdentification/cbc:ID";
				} else {
					if (ublVersion.equals("2.1")) {
						queryXpath = "/sunat:" + typeDoc + "/cac:AccountingSupplierParty/cac:Party/cac:PartyTaxScheme/cbc:CompanyID";
						
						//PAS20181U210300074
						//PAS20181U210300120
						//if("".equals(queryXpath) || queryXpath == null || queryXpath.length() > 20){
						//	queryXpath = "/sunat:" + typeDoc + "/cac:AccountingSupplierParty/cac:Party/cac:PartyIdentification/cbc:ID";
						//}
						
					} else {
						queryXpath = "/sunat:" + typeDoc + "/cac:AccountingSupplierParty/cbc:CustomerAssignedAccountID";
					}
				}
			}
			if (log.isDebugEnabled())
				log.debug("DVDR - queryXpath.toString() = " + queryXpath.toString());
			if (log.isDebugEnabled())
				log.debug("DVDR - doc = " + doc.getXmlVersion().toString());
			if (log.isDebugEnabled())
				log.debug("DVDR - XPathConstants.STRING = " + XPathConstants.STRING);
						
			Object nroRuc = xpath.evaluate(queryXpath, doc, XPathConstants.STRING);

			if (null == nroRuc || "".equals(nroRuc))
				queryXpath = "/sunat:" + typeDoc + "/cac:AccountingSupplierParty/cac:Party/cac:PartyIdentification/cbc:ID";
			
			nroRuc = xpath.evaluate(queryXpath, doc, XPathConstants.STRING);
			
			if (log.isDebugEnabled())
				log.debug("DVDR - nroRuc.toString() = " + nroRuc.toString());
			if (null == nroRuc)
				throw new ServiceException(this, "El documento no tiene el numero de RUC del emisor.");

			queryXpath = "/sunat:" + typeDoc + "/cbc:ID";
			if (log.isDebugEnabled())
				log.debug("DVDR - queryXpath.toString() = " + queryXpath.toString());
			Object serieNro = xpath.evaluate(queryXpath, doc, XPathConstants.STRING);
			if (null == serieNro || serieNro.equals(""))
				throw new ServiceException(this, "El documento no contiene el Número de serie.");

			queryXpath = "/sunat:" + typeDoc + "/cbc:InvoiceTypeCode";
			if(log.isDebugEnabled()) log.debug("DVDR - queryXpath.toString() = " + queryXpath.toString());
			Object codCPETemp = xpath.evaluate(queryXpath, doc,XPathConstants.STRING);
			if(log.isDebugEnabled()) log.debug("DVDR - codCPETemp.toString() = " + codCPETemp.toString());
			
			if ( codCPETemp.toString () != null && ( codCPETemp.toString ().equals ( "30" )
					|| codCPETemp.toString ().equals ( "34" ) || codCPETemp.toString ().equals ( "42" ) ) ) {
				codCpe = codCPETemp.toString ();
			}
			
			if (log.isDebugEnabled())
				log.debug("DVDR - serieNro.toString() = " + serieNro.toString());
			String[] numeros = serieNro.toString().split("-");
			if (numeros.length != 2)
				throw new ServiceException(this, "El documento no tiene un numero de serie correcto.");

			result.put("numRuc", nroRuc.toString().trim());
			// hquispeon En caso el código del comprobante sea EB01 el codCpe debe ser 03
			if (numeros[0].equals("EB01") && codCpe.equals("01")) {
				codCpe = "03";
			}
                        // gmoralesu En caso el código del comprobante sea E001 obtengo el codigoCPE del nvoiceTypeCode
                        if (numeros[0].equals("E001") && codCpe.equals("01")) {
                            String codCpeTypeCodigo = "/sunat:" + typeDoc + "/cbc:InvoiceTypeCode";
                            Object codCpeTag = xpath.evaluate(codCpeTypeCodigo, doc, XPathConstants.STRING);
                            codCpe = codCpeTag.toString();
			}

			result.put("codCpe", codCpe);
			result.put("numSerieCpe", numeros[0]);
			result.put("numCpe", numeros[1]);

			if (log.isDebugEnabled())
				log.debug(">> obtenerDatosComprobante :: " + result);

		} catch (ServiceException se) {
			log.error(se, se);
			throw se;
		} catch (Exception e) {
			log.error(e, e);
			throw new ServiceException(this,
					"No se pudo obtener la información del documento para realizar la verificación, se presenta el error : "
							+ e.getMessage());
		}
		if (log.isDebugEnabled())
			log.debug("DVDR - obtenerDatosComprobante - Fin");
		return result;
	}

	private Date getSignDate(Document doc, XPath xpath) {

		try {
			Date signDate = null;
			String typeDoc = doc.getDocumentElement().getNodeName().trim();
			String queryXpath = "/sunat:" + typeDoc + "/cbc:IssueDate";
			Object fechaEmision = xpath.evaluate(queryXpath, doc, XPathConstants.STRING);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

			signDate = df.parse(fechaEmision.toString());
			return signDate;
		} catch (ParseException e) {
			log.error("Error al formatear la fecha", e);
			throw new RuntimeException("No se pudo encontrar una fecha valida en el documento electronico");
		} catch (XPathExpressionException e) {
			log.error("Error al escribir XPATH de fecha de emision", e);
			throw new RuntimeException(e);
		}

		/*
		 * Buscar la fecha de la firma desde base de datos Map<String,String> params =
		 * this.obtenerDatosComprobante(doc, xpath);
		 * 
		 * T4241Bean param = new T4241Bean(); String nroRUC = params.get("numRuc");
		 * String codCpe = params.get("codCpe"); String nroSerie =
		 * params.get("numSerieCpe"); Integer nroCPE = new
		 * Integer(params.get("numCpe"));
		 * 
		 * t4241DAO.findByPK(nroRUC, nroSerie, nroCPE, codCpe)
		 * 
		 * 
		 */
	}

	/**
	 * Implementación migrada desde el proyecto de Recibo por honorarios
	 * Electónicos.
	 */
	public Map<String, String> evaluarFacturaPORTAL(Document doc, String tipodocumento, ComprobanteParserBean parser)
			throws ServiceException, Exception {
		String respuesta = "Error.";
		// String path = "";
		Map<String, String> mensaje = new HashMap<String, String>();

		try {
			if (tipodocumento.equals("Invoice")) {
				respuesta = validarInvoicePortal(doc, parser);
			} // fin de factura
			if (tipodocumento.equals("DebitNote")) {
				respuesta = validarDebitNotePortal(doc, parser);
			} // fin de Nota de debito
			if (tipodocumento.equals("CreditNote")) {
				respuesta = validarCreditNotePortal(doc, parser);
			} // fin de Nota de Credito
			if (tipodocumento.equals("DespatchAdviceBienFiscalizable")) {
				respuesta = validarDespatchAdviceBienFiscalizablePortal(doc, parser);
			} // fin de GuiaRemision
		} catch (ServiceException e) {
			log.error(e, e);
			mensaje.put("msgError", "Comprobante no valido. " + e.getMessage());
		} catch (ValidateComprobanteException e) {
			mensaje.put("msgError", e.getMessage());
			return mensaje;
		}
		mensaje.put("mensaje", respuesta);

		return mensaje;
	}

	/**
	 * Implementación migrada desde el proyecto de Recibo por honorarios
	 * Electónicos.
	 */
	public Map<String, String> evaluarBoletaPORTAL(Document doc, String tipodocumento, ComprobanteParserBean parser)
			throws ServiceException, Exception {
		String respuesta = "Error.";
		// String path = "";
		Map<String, String> mensaje = new HashMap<String, String>();

		try {
			if (tipodocumento.equals("Invoice")) {
				respuesta = validarInvoiceBoletaPortal(doc, parser);
			} // fin de factura

			if (tipodocumento.equals("CreditNote")) {
				respuesta = validarCreditNoteBoletaPortal(doc, parser);
			} // fin de Nota de Credito

			if (tipodocumento.equals("DebitNote")) {
				respuesta = validarDebitNoteBoletaPortal(doc, parser);
			} // fin de Nota de debito
		} catch (ValidateComprobanteException e) {
			mensaje.put("msgError", e.getMessage());
			return mensaje;
		}
		mensaje.put("mensaje", respuesta);

		return mensaje;
	}

	private String validarDespatchAdviceBienFiscalizablePortal(Document doc, ComprobanteParserBean parser)
			throws ServiceException, Exception {
		String respuesta = "";
		String path;
		log.debug("Documento a evaluar: Guia de remision bienes fiscalizables");
		String xserienum = getDocumentIdPortal(doc, parser);
		log.debug("XSERIENUM:" + xserienum);

		String estadoDoc = "";
		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		if (existeCdpActivo("DespatchAdviceBienFiscalizable", doc, parser)) {
			estadoDoc = "<br>Estado del Documento: Activo";
		} else {
			estadoDoc = "<br>Estado del Documento: Baja";
		}

		// NodeList versionXmlSunat = doc.getElementsByTagName("cbc:CustomizationID");
		// boolean existeVersion = true;
		// existeVersion = false;
		String schema = parser.getSchema();
		path = "/data0/see/emision/gre/resources/maindoc/" + schema;

		if (log.isDebugEnabled()) {
			log.debug(">> Schema recuperado: " + schema);
			log.debug("path2:" + path);
			log.debug("parser es: " + parser.getClass());
		}
		parser.setSchemaSource(new StreamSource(new File(path)));
		Date signDate = getSignDate(doc, parser.getXPath());
		VerifySignResult verifySignResult = parser.validate(false, signDate);
		respuesta = respuestaToString(verifySignResult);

		respuesta = "Guía de remisión: " + xserienum + " - " + respuesta + estadoDoc;
		return respuesta;
	}

	/**
	 * Valida un anota de credito
	 * 
	 * @param doc
	 * @param parser
	 * @param schema
	 * @return
	 * @throws Exception
	 */
	private String validarCreditNotePortal(Document doc, ComprobanteParserBean parser) throws Exception {
		String respuesta = "";
		String path;
		log.debug("Documento a evaluar: Nota de credito");
		String xserienum = getDocumentIdPortal(doc, parser);
		log.debug("XSERIENUM:" + xserienum);

		String estadoDoc = "";
		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		if (existeCdpActivo("CreditNote", doc, parser)) {
			estadoDoc = "<br>Estado del Documento: Activo";
		} else {
			estadoDoc = "<br>Estado del Documento: Baja";
		}

		// NodeList versionXmlSunat = doc.getElementsByTagName("cbc:CustomizationID");
		boolean existeVersion = true;
		existeVersion = false;
		String schema = parser.getSchema();
		if (log.isDebugEnabled())
			log.debug(">> Schema recuperado: " + schema);
		path = "/data0/see/emision/notacredito/resources/maindoc/" + schema;
		// path = "/data0/compelec/resources/maindoc/"+schema;

		log.debug("path2:" + path);
		// parser.setSchemaSource(new StreamSource(new File(path)));
		parser.setSchemaSource(new StreamSource(new File(path)));
		// parser.validate(properties);

		log.debug("existeVersion: " + existeVersion);
		log.debug("parser:  paso");
		log.debug("parser es: " + parser.getClass());

		Date signDate = getSignDate(doc, parser.getXPath());

		VerifySignResult verifySignResult = parser.validate(!existeVersion, signDate);

		respuesta = respuestaToString(verifySignResult);
		respuesta = "Nota de Crédito: " + xserienum + " - " + respuesta + estadoDoc;
		return respuesta;
	}

	/**
	 * Valida un anota de credito boleta
	 * 
	 * @param doc
	 * @param parser
	 * @param schema
	 * @return
	 * @throws Exception
	 */
	private String validarCreditNoteBoletaPortal(Document doc, ComprobanteParserBean parser) throws Exception {
		String respuesta = "";
		String path;
		log.debug("Documento a evaluar: Nota de credito boleta");
		String xserienum = getDocumentIdPortal(doc, parser);
		log.debug("XSERIENUM:" + xserienum);

		String estadoDoc = "";
		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		if (existeCdpActivo("CreditNote", doc, parser)) {
			estadoDoc = "<br>Estado del Documento: Activo";
		} else {
			estadoDoc = "<br>Estado del Documento: Baja";
		}

		// NodeList versionXmlSunat = doc.getElementsByTagName("cbc:CustomizationID");
		boolean existeVersion = true;
		existeVersion = false;
		String schema = parser.getSchema();
		if (log.isDebugEnabled())
			log.debug(">> Schema recuperado: " + schema);
		path = "/data0/see/emision/notacreditobve/resources/maindoc/" + schema;
		// path = "/data0/compelec/resources/maindoc/"+schema;

		log.debug("path2:" + path);
		// parser.setSchemaSource(new StreamSource(new File(path)));
		parser.setSchemaSource(new StreamSource(new File(path)));
		// parser.validate(properties);

		log.debug("existeVersion: " + existeVersion);
		log.debug("parser:  paso");
		log.debug("parser es: " + parser.getClass());

		Date signDate = getSignDate(doc, parser.getXPath());

		VerifySignResult verifySignResult = parser.validate(!existeVersion, signDate);

		respuesta = respuestaToString(verifySignResult);
		respuesta = "Nota de Crédito: " + xserienum + " - " + respuesta + estadoDoc;
		return respuesta;
	}

	/**
	 * Valida un anota de debito
	 * 
	 * @param doc
	 * @param parser
	 * @param schema
	 * @return
	 * @throws Exception
	 */
	private String validarDebitNotePortal(Document doc, ComprobanteParserBean parser) throws Exception {
		String respuesta = "";
		String path;
		log.debug("Documento a evaluar: Nota de debito");
		String xserienum = getDocumentIdPortal(doc, parser);
		log.debug("XSERIENUM:" + xserienum);

		String estadoDoc = "";
		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		if (existeCdpActivo("DebitNote", doc, parser)) {
			estadoDoc = "<br>Estado del Documento: Activo";
		} else {
			estadoDoc = "<br>Estado del Documento: Baja";
		}

		// NodeList versionXmlSunat = doc.getElementsByTagName("cbc:CustomizationID");
		boolean existeVersion = true;
		existeVersion = false;
		String schema = parser.getSchema();
		if (log.isDebugEnabled())
			log.debug(">> Schema recuperado: " + schema);
		path = "/data0/see/emision/notadebito/resources/maindoc/" + schema;
		// path = "/data0/compelec/resources/maindoc/"+schema;

		log.debug("path2:" + path);
		// parser.setSchemaSource(new StreamSource(new File(path)));
		parser.setSchemaSource(new StreamSource(new File(path)));

		log.debug("existeVersion: " + existeVersion);
		log.debug("parser:  paso");
		log.debug("parser es: " + parser.getClass());

		Date signDate = getSignDate(doc, parser.getXPath());

		VerifySignResult verifySignResult = parser.validate(!existeVersion, signDate);

		respuesta = respuestaToString(verifySignResult);

		respuesta = "Nota de Débito: " + xserienum + " - " + respuesta + estadoDoc;
		return respuesta;
	}

	/**
	 * Valida un anota de debito para boleta
	 * 
	 * @param doc
	 * @param parser
	 * @param schema
	 * @return
	 * @throws Exception
	 */
	private String validarDebitNoteBoletaPortal(Document doc, ComprobanteParserBean parser) throws Exception {
		String respuesta = "";
		String path;
		log.debug("Documento a evaluar: Nota de debito boleta");
		String xserienum = getDocumentIdPortal(doc, parser);
		log.debug("XSERIENUM:" + xserienum);

		String estadoDoc = "";
		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		if (existeCdpActivo("DebitNote", doc, parser)) {
			estadoDoc = "<br>Estado del Documento: Activo";
		} else {
			estadoDoc = "<br>Estado del Documento: Baja";
		}

		// NodeList versionXmlSunat = doc.getElementsByTagName("cbc:CustomizationID");
		boolean existeVersion = true;
		existeVersion = false;
		String schema = parser.getSchema();
		if (log.isDebugEnabled())
			log.debug(">> Schema recuperado: " + schema);
		path = "/data0/see/emision/notadebitobve/resources/maindoc/" + schema;
		// path = "/data0/compelec/resources/maindoc/"+schema;

		log.debug("path2:" + path);
		// parser.setSchemaSource(new StreamSource(new File(path)));
		parser.setSchemaSource(new StreamSource(new File(path)));

		log.debug("existeVersion: " + existeVersion);
		log.debug("parser:  paso");
		log.debug("parser es: " + parser.getClass());

		Date signDate = getSignDate(doc, parser.getXPath());

		VerifySignResult verifySignResult = parser.validate(!existeVersion, signDate);

		respuesta = respuestaToString(verifySignResult);

		respuesta = "Nota de Débito: " + xserienum + " - " + respuesta + estadoDoc;
		return respuesta;
	}

	/**
	 * Validar una factura
	 * 
	 * @param doc
	 * @param parser
	 * @param schema
	 * @return
	 * @throws ServiceException
	 * @throws Exception
	 */
	private String validarInvoicePortal(Document doc, ComprobanteParserBean parser) throws ServiceException, Exception {
		String respuesta = "";
		String path;
		log.debug("Documento a evaluar: Invoice");
		String xserienum = getDocumentIdPortal(doc, parser);
		boolean esFactura = esInvoiceFactura(doc, parser);
		log.debug("XSERIENUM:" + xserienum);

		String estadoDoc = "";
		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		if (existeCdpActivo("Invoice", doc, parser)) {
			estadoDoc = "<br>Estado del Documento: Activo";
		} else {
			estadoDoc = "<br>Estado del Documento: Baja";
		}

		NodeList tipoDoc = doc.getElementsByTagName("cbc:InvoiceTypeCode");
		log.debug("tipoDoc:" + tipoDoc);

		log.debug("esFactura:" + esFactura);

		if (tipoDoc != null && tipoDoc.getLength() > 0) {
			Element fstElmnt = (Element) tipoDoc.item(0);
			NodeList fstNm = fstElmnt.getChildNodes();
			log.debug("Tipo de XML cbc ------> " + ((Node) fstNm.item(0)).getNodeValue());
			if (!(((Node) fstNm.item(0)).getNodeValue()).equals("0100") && // Factura regular
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0101") && // Venta Gratuita
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0102") && // Exportacion
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0103") && // Anticipo
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0104") && // Factura regular, nuevo formato
					!(((Node) fstNm.item(0)).getNodeValue()).equals("01")) {
				throw new ServiceException(this,
						"Sólo archivos Digitales de Facturas Electrónicas pueden ser Verificados.");
			}
		} else {
			log.debug("Tipo de XML cbc ------> NULL");
			throw new ServiceException(this,
					"Sólo archivos Digitales de Facturas Electrónicas pueden ser Verificados.");
		}

		NodeList versionXmlSunat = doc.getElementsByTagName("cbc:CustomizationID");
		boolean existeVersion = true;
		if (versionXmlSunat != null && versionXmlSunat.getLength() > 0) {
			Element fstElmnt = (Element) versionXmlSunat.item(0);
			NodeList fstNm = fstElmnt.getChildNodes();
			log.debug("Version de XML ------> " + ((Node) fstNm.item(0)).getNodeValue());
			existeVersion = false; // MPCR Quitar cuando se pueda validar el
			// XML
		} else {
			log.debug("Version de XML ------> 1.0 ");
			existeVersion = false;
		}
		String schema = parser.getSchema();
		if (log.isDebugEnabled())
			log.debug(">> Schema recuperado: " + schema);
		path = "/data0/see/emision/factura/resources/maindoc/" + schema;
		// path = "/data0/compelec/resources/maindoc/"+schema;

		log.debug("path2:" + path);
		// parser.setSchemaSource(new StreamSource(new File(path)));
		parser.setSchemaSource(new StreamSource(new File(path)));
		// parser.validate(properties);

		log.debug("existeVersion: " + existeVersion);//existeVersion: false
		log.debug("parser:  paso");
		log.debug("parser es: " + parser.getClass());//class pe.gob.sunat.servicio2.registro.electronico.comppago.factura.bean.ComprobanteParserBean
		Date signDate = getSignDate(doc, parser.getXPath());

		VerifySignResult verifySignResult = parser.validate(!existeVersion, signDate);
		log.debug("verifySignResult: "+verifySignResult);
		respuesta = respuestaToString(verifySignResult);
		if (esFactura) {
			respuesta = "Factura: " + xserienum + " - " + respuesta + estadoDoc;
		} else {
			respuesta = "Recibo por honorario: " + xserienum + " - " + respuesta + estadoDoc;
		}
		return respuesta;
	}

	/**
	 * Validar una boleta
	 * 
	 * @param doc
	 * @param parser
	 * @param schema
	 * @return
	 * @throws ServiceException
	 * @throws Exception
	 */
	private String validarInvoiceBoletaPortal(Document doc, ComprobanteParserBean parser)
			throws ServiceException, Exception {
		String respuesta = "";
		String path;
		log.debug("Documento a evaluar: Invoice");
		String xserienum = getDocumentIdPortal(doc, parser);
		log.debug("XSERIENUM:" + xserienum);

		String estadoDoc = "";
		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		if (existeCdpActivo("Invoice", doc, parser)) {
			estadoDoc = "<br>Estado del Documento: Activo";
		} else {
			estadoDoc = "<br>Estado del Documento: Baja";
		}

		NodeList tipoDoc = doc.getElementsByTagName("cbc:InvoiceTypeCode");
		log.debug("tipoDoc:" + tipoDoc);
		if (tipoDoc != null && tipoDoc.getLength() > 0) {
			Element fstElmnt = (Element) tipoDoc.item(0);
			NodeList fstNm = fstElmnt.getChildNodes();
			log.debug("Tipo de XML cbc ------> " + ((Node) fstNm.item(0)).getNodeValue());
			if (!(((Node) fstNm.item(0)).getNodeValue()).equals("0100") && // Factura regular
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0101") && // Venta Gratuita
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0102") && // Exportacion
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0103") && // Anticipo
					!(((Node) fstNm.item(0)).getNodeValue()).equals("0104") && // Factura regular, nuevo formato
					!(((Node) fstNm.item(0)).getNodeValue()).equals("01") && // Boleta regular, nuevo formato
					!(((Node) fstNm.item(0)).getNodeValue()).equals("03")) {
				throw new ServiceException(this,
						"Sólo archivos Digitales de Boletas Electrónicas pueden ser Verificados.");
			}
		} else {
			log.debug("Tipo de XML cbc ------> NULL");
			throw new ServiceException(this, "Sólo archivos Digitales de Boletas Electrónicas pueden ser Verificados.");
		}

		NodeList versionXmlSunat = doc.getElementsByTagName("cbc:CustomizationID");
		boolean existeVersion = true;
		if (versionXmlSunat != null && versionXmlSunat.getLength() > 0) {
			Element fstElmnt = (Element) versionXmlSunat.item(0);
			NodeList fstNm = fstElmnt.getChildNodes();
			log.debug("Version de XML ------> " + ((Node) fstNm.item(0)).getNodeValue());
			existeVersion = false; // MPCR Quitar cuando se pueda validar el
			// XML
		} else {
			log.debug("Version de XML ------> 1.0 ");
			existeVersion = false;
		}
		String schema = parser.getSchema();
		if (log.isDebugEnabled())
			log.debug(">> Schema recuperado: " + schema);
		path = "/data0/see/emision/boleta/resources/maindoc/" + schema;
		// path = "/data0/compelec/resources/maindoc/"+schema;

		log.debug("path2:" + path);
		// parser.setSchemaSource(new StreamSource(new File(path)));
		parser.setSchemaSource(new StreamSource(new File(path)));
		// parser.validate(properties);

		log.debug("existeVersion: " + existeVersion);
		log.debug("parser:  paso");
		log.debug("parser es: " + parser.getClass());
		Date signDate = getSignDate(doc, parser.getXPath());

		VerifySignResult verifySignResult = parser.validate(!existeVersion, signDate);

		respuesta = respuestaToString(verifySignResult);
		respuesta = "Boleta: " + xserienum + " - " + respuesta + estadoDoc;
		return respuesta;
	}

	private String getDocumentIdPortal(Document doc, ComprobanteParserBean parser) throws XPathExpressionException {
		String typeDoc = doc.getDocumentElement().getNodeName().trim();
		XPath xpath = parser.getXPath();
		String queryXpath = "/sunat:" + typeDoc + "/cbc:ID";
		Object serieNro = xpath.evaluate(queryXpath, doc, XPathConstants.STRING);

		String xserienum = serieNro.toString();
		return xserienum;
	}

	private boolean esInvoiceFactura(Document doc, ComprobanteParserBean parser) throws XPathExpressionException {
		boolean esfactura = true;
		String typeDoc = doc.getDocumentElement().getNodeName().trim();
		XPath xpath = parser.getXPath();
		String queryXpathUblVersion = "/sunat:" + typeDoc + "/cbc:UBLVersionID";
		String queryXpathUblCustom = "/sunat:" + typeDoc + "/cbc:CustomizationID";

		String ublVersion = (String) xpath.evaluate(queryXpathUblVersion, doc, XPathConstants.STRING);
		String ublCustom = (String) xpath.evaluate(queryXpathUblCustom, doc, XPathConstants.STRING);

		if (ublVersion == null || ublCustom == null || ublVersion.length() <= 0 || ublCustom.length() <= 0) {
			esfactura = false;
		} else {
			log.debug(">>ublVersion=[" + ublVersion + "]");
			log.debug(">>ublCustom=[" + ublCustom + "]");
		}
		return esfactura;
	}

	private boolean existeCdpActivo(String tipodocumento, Document doc, ComprobanteParserBean parser)
			throws XPathExpressionException {

		boolean rsp = true;

		// MSF: se evalua si el cdp existe en BD y si esta de baja.
		Map<String, String> params = this.obtenerDatosComprobante(doc, parser.getXPath());
		String numRuc = params.get("numRuc");
		String serie = params.get("numSerieCpe");
		Integer numCpe = new Integer(params.get("numCpe"));
		String codCpe = params.get("codCpe");

		if (tipodocumento.equals("Invoice")) {
			if (esInvoiceFactura(doc, parser)) {
                                if(!codCpe.equals("23")){ // Antonio - Poliza de Adjudicacion Electronica
                                    T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
                                    if (null != t4241Bean) {
                                            if (t4241Bean.getInd_estado().trim().equals(BAJACPE)) {
                                                    // throw new ServiceException(this,"El documento (Factura) con número de serie
                                                    // "+ serie+"-"+numCpe+" se encuentra de baja.");
                                                    rsp = false;
                                            }
                                    } else {
                                            throw new ServiceException(this, "El documento con número de serie::: " + serie + "-" + numCpe
                                                            + " no ha sido informado a SUNAT.");
                                    }
                                }else{ // Antonio - Poliza de Adjudicacion Electronica
                                    T10194Bean filtroBean= new T10194Bean();
                                    filtroBean.setNum_ruc(numRuc);
                                    filtroBean.setNum_serie_cpe(serie);
                                    filtroBean.setNum_cpe(numCpe);
                                    filtroBean.setCod_cpe(codCpe);
                                    T10194Bean t4241Bean = t10194DAO.findByFiltroRucSerieCodNum(filtroBean);
                                    if (null != t4241Bean) {
                                            if (t4241Bean.getInd_baja().trim().equals("1")) {
                                                    rsp = false;
                                            }
                                    } else {
                                            throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe
                                                            + " no se encuentra registrado en SUNAT.");
                                    }
                                }
			} else {
				// Es RHE
				T3639Bean t3639Bean = t3639DAO.findByPk(numRuc, serie, "01", numCpe);
				if (null != t3639Bean) {
					if (!t3639Bean.getInd_estado_rec().trim().equals(ACTIVORHE)) {
						// throw new ServiceException(this,"El documento (RHE) con número de serie
						// "+serie+"-"+numCpe+" se encuentra de baja.");
						rsp = false;
					}
				} else {
					throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe
							+ " no se encuentra registrado en SUNAT.");
				}
			}
		} else {
			T4241Bean t4241Bean = t4241DAO.findByPK(numRuc, serie, numCpe, codCpe);
			if (null != t4241Bean) {
				if (tipodocumento.equals("DespatchAdviceBienFiscalizable")) {
					if (t4241Bean.getInd_estado().trim().equals(BAJAGREBF)) {
						// throw new ServiceException(this,"El documento (GRE-BF) con número de serie
						// "+serie+"-"+numCpe+" se encuentra de baja.");
						rsp = false;
					}
				} else {
					if (t4241Bean.getInd_estado().trim().equals(BAJACPE)) {
						// throw new ServiceException(this,"El documento con número de serie
						// "+serie+"-"+numCpe+" se encuentra de baja.");
						rsp = false;
					}
				}
			} else {
				throw new ServiceException(this, "El documento con número de serie " + serie + "-" + numCpe
						+ " no se encuentra registrado en SUNAT.");
			}
		}
		return rsp;
	}

	public class ValidateComprobanteException extends Exception {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8787384240682092827L;

		ValidateComprobanteException(String message) {
			super(message);
		}
	}

	/**
	 * Transforma el codigo del mensaje en un mensaje entendible
	 * 
	 * @param codigoRespuesta
	 * @return
	 * @throws ValidateComprobanteException
	 */
	private String respuestaToString(VerifySignResult verifySignResult) throws ValidateComprobanteException {
		String respuesta = "";
		int codigoRespuesta = verifySignResult.getResultCode();
		if (codigoRespuesta == -1) {
			// respuesta = "Hubo un error en la validacion del documento";
			throw new ValidateComprobanteException(verifySignResult.getMessage());

		} else if (codigoRespuesta == 0) {
			respuesta = "El documento no fue firmado por SUNAT o presenta firmas múltiples.";
		} else if (codigoRespuesta == 1) {
			respuesta = "El Documento Electrónico ingresado ha sido generado por el Sistema de Emisión Electrónica de SUNAT, es íntegro, auténtico y cumple con el estándar establecido.";
		} else if (codigoRespuesta == 2) {
			respuesta = "El documento no presenta firma digital.";
		} else if (codigoRespuesta == 3) {
			respuesta = "El documento fue modificado luego de ser firmado por SUNAT.";
		} else if (codigoRespuesta == 4) {
			respuesta = "El documento no es valido por que fue firmado con un certificado vencido.";
		}
		return respuesta;
	}
	
	
	

	public String buscarArchivoNFS(String textoJson) {
        if (log.isDebugEnabled()) log.debug("Inicio - consultarNFS textoJson: "+textoJson);
        String resp = "";
        //String url = "http://cpeose-tkgi.k8s.sunat.peru/v1/contribuyentems2/registro/cpe/consulta/nfs/individual";

        // JSON que quieres enviar
        String jsonInputString = textoJson;
        HttpURLConnection con = null;
        BufferedReader br = null;
        OutputStream os = null;
 
        try {
        	if (log.isDebugEnabled()) log.debug("consultarNFS - URI - inicio");
        	T01Bean pbean = t01DAO.findByNumeroArgumento("967", "INTRES16152601");
			if (log.isDebugEnabled()) log.debug("consultarNFS - URI - fin");
			String s_uri=pbean.getFuncion().substring(0, 119).trim();
			String s_estadoWS=pbean.getFuncion().substring(124, 125).trim();
			if("0".equals(s_estadoWS)) {				
				throw new RuntimeException("Servicio Web se encuentra inhabilitado.WS:"+"INTRES16152601");
			}
			if (log.isDebugEnabled()) log.debug("consultarNFS - URI "+ s_uri);			
        	
        	
            //URL urlObj = new URL(url);
			URL urlObj = new URL(s_uri);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);
 
            // Envío del JSON
            os = con.getOutputStream();
            byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
            if (log.isDebugEnabled()) log.debug("output: "+os);
            
            // Lectura de la respuesta
            br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            resp = response.toString();
            if (log.isDebugEnabled()) log.debug("ConsultarNFS resp: "+resp);
 
            // Procesar la respuesta JSON para extraer el valor base64
            JSONObject jsonObject = new JSONObject(resp);
            JSONArray resultsArray = jsonObject.getJSONArray("results");
 
            // Suponiendo que solo hay un objeto en el array "results"
            JSONObject resultObject = resultsArray.getJSONObject(0);
            byte[] decodedBytes = null;
            String decodedString = null;
			if (resultsArray.length() > 0) {
                JSONObject firstResult = resultsArray.getJSONObject(0);
                if (log.isDebugEnabled()) log.debug("firstResult: "+firstResult);
                
                // Obtener el valor base64
                String base64String = firstResult.getString("base64");
                if (log.isDebugEnabled()) log.debug("base64String: "+base64String);
                
                // Decodificar el valor base64
    			if(System.getProperty("java.version").startsWith("1.7")) {
    				decodedBytes = new BASE64Decoder().decodeBuffer(base64String); // JAVA 6+
    				decodedBytes=decompress(decodedBytes);
    				//return DatatypeConverter.parseBase64Binary(encoded); // JAVA 6+
    				if (log.isDebugEnabled()) log.debug("decodedBytes-1.7: "+decodedBytes);
    				// Eliminar posibles caracteres BOM y espacios en blanco al principio del XML
    				decodedString = new String(decodedBytes, "UTF-8");
    				if (log.isDebugEnabled()) log.debug("decodedString-1.7: "+decodedString);
    			}
    			else {
    					decodedBytes = Base64.getDecoder().decode(base64String); // JAVA 8+
    					decodedBytes=decompress(decodedBytes);
    					if (log.isDebugEnabled()) log.debug("decodedBytes-8: "+decodedBytes);
    					// Eliminar posibles caracteres BOM y espacios en blanco al principio del XML
    					decodedString = new String(decodedBytes, "UTF-8");
    					if (log.isDebugEnabled()) log.debug("decodedString-8: "+decodedString);
    			}
			}   

            return decodedString;

            
        } catch (Exception e) {
        	if (log.isDebugEnabled()) log.debug("consultarNFS - Except: "+e);
        		return null;
        } finally {
        			if (os != null) {
        				try {
        					os.close();
        				} catch (Exception e) 
        					{ if (log.isDebugEnabled()) log.debug("consultarNFS - Except os: "+e);}
        			}
        			if (br != null) {
        				try {
        					br.close();
        				} catch (Exception e) {
        					if (log.isDebugEnabled()) log.debug("consultarNFS - Except br: "+e);
        				}
        			}
        			if (con != null) {
        				con.disconnect();
        			}
        }
	}
	
	private T4243Bean buscarArchivoNube(String numRuc, String codCpe, String numSerieCpe, Integer numCpe, byte[] bytes) throws IOException {
		T4243Bean t4243Bean = null;
		// sino esta el ind_modo = 1 hay q insertar
		// obtengo num_ticket y num_correl_ticket
		
		T4536Bean t4536bean = null;
		////INICIO - PAS20201U210100038
		if (codCpe.equals ( "30" ) || codCpe.equals ( "42" )) {
			t4536bean = t10204DAO.findByDataDAE ( numRuc, codCpe, numSerieCpe, numCpe );
		} else if (codCpe.equals ( "34" )) {
			t4536bean = t10209DAO.findByDataDAE ( numRuc, codCpe, numSerieCpe, numCpe );
		}else {
			t4536bean = t4243DAO.findFileZipJoinTCabCPETRelcompelecByPrimaryKey(numRuc, codCpe, numSerieCpe, numCpe);
		}
		////FIN - PAS20201U210100038

		if (t4536bean != null) {

			// buscar xml en festore de cpe
			
		    ////INICIO - PAS20201U210100038
			T4536Bean t4536Festore = null;
			if ( codCpe.equals ( "30" ) || codCpe.equals ( "34" ) || codCpe.equals ( "42" ) ) {
				t4536Festore = daoT4536.findFileZipTFESTOREByPrimaryKey ( t4536bean.getTicket ().trim () );
			} else {
				t4536Festore = daoT4536.findFileZipTFESTOREByPrimaryKey( t4536bean );
			}
		    ////FIN - PAS20201U210100038

			if (t4536Festore != null) {
				t4243Bean = new T4243Bean();
				t4243Bean.setArc_zip(t4536Festore.getContenido());
				t4243Bean.setDes_nombre(daoT4536.findNombre(t4536bean.getTicket()));
			} else {

				T4536Bean t4536storeHistorico = daoT4536Hist.findFileZipTFESTOREByPrimaryKey( t4536bean );

				if (t4536storeHistorico != null) {
					t4243Bean = new T4243Bean();
					t4243Bean.setArc_zip(t4536storeHistorico.getContenido());
				} else {
					
					if(log.isDebugEnabled()) log.debug("Consultando cloud - get xml buscarArchivoNube");
					byte[] xmlCloud = null;
					
					T4283Bean buscarT4283 = new T4283Bean();
					buscarT4283.setNum_ruc(numRuc);
					buscarT4283.setCod_cpe(codCpe);
					buscarT4283.setNum_serie_cpe(numSerieCpe);
					buscarT4283.setNum_cpe(numCpe);
					buscarT4283.setCod_rubro(IND_RUBRO_CLOUD_GUID); 
					
					T4283Bean rubro = t4283DAO.findRubroCabecera_ByRUC_codCPE_Serie_CPE_Rubro(buscarT4283);
					String guid = "";
					if (rubro != null) {
						guid = rubro.getDes_detalle_rubro();
					}	
					try {
						if(log.isDebugEnabled()) log.debug("Consultando procesarComprobanteNubev2_0 buscarArchivoNube");
						xmlCloud = this.procesarComprobanteNubev2_0(guid);
						if(log.isDebugEnabled()) log.debug("FIN Consultando procesarComprobanteNubev2_0 buscarArchivoNube");
					} catch (Exception e) {
						throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					}
					if(log.isDebugEnabled()) log.debug("xmlCloud ===>"+xmlCloud);
					
					if(xmlCloud.length > 0){
						t4243Bean = new T4243Bean();
					//	byte[] bytesTmp = xmlCloud.getBytes("ISO-8859-1");
						String nombreZip = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe+".xml";
						//t4243Bean.setArc_xml(xmlCloud);
						try {
						//	t4243Bean.setArc_zip(zipBytes( nombreZip, xmlCloud ));
							t4243Bean.setArc_zip(xmlCloud);
							
							T4534Bean T4534Bean = t4534DAO.findByPK(numRuc, numSerieCpe, codCpe, numCpe);
							String arrayZip = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe+".xml";

							
							BillStore BillStore_ = new BillStore();
							BillStore_.setTicket(T4534Bean.getNum_ticket());
							BillStore_.setModo("1");
							BillStore_.setContenido(xmlCloud);
							BillStore_.setCorrelativo(T4534Bean.getNum_correl_ticket());
							BillStore_.setUsuarioModificador(T4534Bean.getCod_usumodif());
							t4243DAOSpring.insert_ticket_CPE(BillStore_);						
							
						} catch (Exception e) {
							e.printStackTrace();
							throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
						}
					}
					else
					{
						if(log.isDebugEnabled()) log.debug("===xmlCloud.length es 0");
						throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					}
				}

			}

		}

		return t4243Bean;
	}
	private T4243Bean buscarArchivoGem(String numRuc, String codCpe, String numSerieCpe, Integer numCpe, byte[] bytes) throws IOException {

		T4243Bean t4243Bean = null;
		// sino esta el ind_modo = 1 hay q insertar
		// obtengo num_ticket y num_correl_ticket
		
		T4536Bean t4536bean = null;
		////INICIO - PAS20201U210100038
		if (codCpe.equals ( "30" ) || codCpe.equals ( "42" )) {
			t4536bean = t10204DAO.findByDataDAE ( numRuc, codCpe, numSerieCpe, numCpe );
		} else if (codCpe.equals ( "34" )) {
			t4536bean = t10209DAO.findByDataDAE ( numRuc, codCpe, numSerieCpe, numCpe );
		}else {
			t4536bean = t4243DAO.findFileZipJoinTCabCPETRelcompelecByPrimaryKey(numRuc, codCpe, numSerieCpe, numCpe);
		}
		////FIN - PAS20201U210100038

		if (t4536bean != null) {

			// buscar xml en festore de cpe
			
		    ////INICIO - PAS20201U210100038
			T4536Bean t4536Festore = null;
			if ( codCpe.equals ( "30" ) || codCpe.equals ( "34" ) || codCpe.equals ( "42" ) ) {
				t4536Festore = daoT4536.findFileZipTFESTOREByPrimaryKey ( t4536bean.getTicket ().trim () );
			} else {
				t4536Festore = daoT4536.findFileZipTFESTOREByPrimaryKey( t4536bean );
			}
		    ////FIN - PAS20201U210100038

			if (t4536Festore != null) {
				t4243Bean = new T4243Bean();
				t4243Bean.setArc_zip(t4536Festore.getContenido());
				t4243Bean.setDes_nombre(daoT4536.findNombre(t4536bean.getTicket()));
			} else {

				T4536Bean t4536storeHistorico = daoT4536Hist.findFileZipTFESTOREByPrimaryKey( t4536bean );

				if (t4536storeHistorico != null) {
					t4243Bean = new T4243Bean();
					t4243Bean.setArc_zip(t4536storeHistorico.getContenido());
				} else {
						T4534Bean T4534Bean = t4534DAO.findByPK(numRuc, numSerieCpe, codCpe, numCpe);
						String arrayZip = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe+".xml";

						BillStore BillStore_ = new BillStore();
						BillStore_.setTicket(T4534Bean.getNum_ticket());
						BillStore_.setModo("1");
						BillStore_.setContenido(zipBytes(arrayZip, bytes));
						BillStore_.setCorrelativo(T4534Bean.getNum_correl_ticket());
						BillStore_.setUsuarioModificador(T4534Bean.getCod_usumodif());

						t4243DAOSpring.insert_ticket_CPE(BillStore_);
						
						t4243Bean = new T4243Bean();
						t4243Bean.setArc_zip(zipBytes(arrayZip, bytes));
				}

			}

		}

		return t4243Bean;
	}
	
		
	private T4243Bean buscarArchivoGemConGrabado(String numRuc, String codCpe, String numSerieCpe, Integer numCpe, Document doc, ComprobanteParserBean parser, byte[] bytes) throws IOException{
		
		T4243Bean t4243Bean = null;
		boolean isGem = false;
		
		if (numSerieCpe.toUpperCase().startsWith("F") || numSerieCpe.toUpperCase().startsWith("B")) {
			isGem = true;
		}

		if(isGem) {
			
			if(log.isDebugEnabled()) log.debug("Consultando gem db cpe - get ticket, correlativo y firma");
			T4536Bean t4536bean = null;
			////INICIO - PAS20201U210100038
			if (codCpe.equals ( "30" ) || codCpe.equals ( "42" )) {
				t4536bean = t10204DAO.findByDataDAE ( numRuc, codCpe, numSerieCpe, numCpe );
			} else if (codCpe.equals ( "34" )) {
				t4536bean = t10209DAO.findByDataDAE ( numRuc, codCpe, numSerieCpe, numCpe );
			}else {
				t4536bean = t4243DAO.findFileZipJoinTCabCPETRelcompelecByPrimaryKey(numRuc, codCpe, numSerieCpe, numCpe);
			}
			////FIN - PAS20201U210100038
			
			if (null != t4536bean) {
				
				String nombreXml = formatNombreXml( daoT4536.findNombre(t4536bean.getTicket()), numRuc, codCpe, numSerieCpe, numCpe );

				if(log.isDebugEnabled()) log.debug("Consultando db cpe - Festore get xml"); 
				////INICIO - PAS20201U210100038
				T4536Bean t4536Festore = null;
				if ( codCpe.equals ( "30" ) || codCpe.equals ( "34" ) || codCpe.equals ( "42" ) ) {
					t4536Festore = daoT4536.findFileZipTFESTOREByPrimaryKey ( t4536bean.getTicket ().trim () );
				} else {
					t4536Festore = daoT4536.findFileZipTFESTOREByPrimaryKey( t4536bean );
				}
			    ////FIN - PAS20201U210100038

				if (t4536Festore != null) {
					t4243Bean = new T4243Bean();
					t4243Bean.setArc_zip(t4536Festore.getContenido());
					
				} else {
					
					if(log.isDebugEnabled()) log.debug("Consultando db hist - Festore get xml");
					T4536Bean t4536festoreHistorico = daoT4536Hist.findFileZipTFESTOREByPrimaryKey( t4536bean );

					if (t4536festoreHistorico != null) {
						
						t4243Bean = new T4243Bean();
						t4243Bean.setArc_zip(t4536festoreHistorico.getContenido());
						
					} else {
						
						if(log.isDebugEnabled()) log.debug("Consultando cloud - get xml");
						byte[] xmlCloud = null;
						
						T4283Bean buscarT4283 = new T4283Bean();
						buscarT4283.setNum_ruc(numRuc);
						buscarT4283.setCod_cpe(codCpe);
						buscarT4283.setNum_serie_cpe(numSerieCpe);
						buscarT4283.setNum_cpe(numCpe);
						buscarT4283.setCod_rubro(IND_RUBRO_CLOUD_GUID); 
						
						T4283Bean rubro = t4283DAO.findRubroCabecera_ByRUC_codCPE_Serie_CPE_Rubro(buscarT4283);
						String guid = "";
						if (rubro != null) {
							guid = rubro.getDes_detalle_rubro();
						}
							
						try {
							if(log.isDebugEnabled()) log.debug("Consultando procesarComprobanteNubev2_0 buscarArchivoGemConGrabado");
							xmlCloud = this.procesarComprobanteNubev2_0(guid);
							if(log.isDebugEnabled()) log.debug("FIN Consultando procesarComprobanteNubev2_0 buscarArchivoGemConGrabado");
							
							
							

							if(log.isDebugEnabled()) log.debug("xmlCloud ===>"+xmlCloud);
							
							if(xmlCloud.length > 0){
								t4243Bean = new T4243Bean();
						//	byte[] bytesTmp = xmlCloud.getBytes("ISO-8859-1");
							String nombreZip = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe+".xml";
							//t4243Bean.setArc_xml(xmlCloud);
				
							//	t4243Bean.setArc_zip(zipBytes( nombreZip, xmlCloud ));
								t4243Bean.setArc_zip(xmlCloud);
								
								T4534Bean T4534Bean = t4534DAO.findByPK(numRuc, numSerieCpe, codCpe, numCpe);
								String arrayZip = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe+".xml";
									/*
								ajj
								BillStore BillStore_ = new BillStore();
								BillStore_.setTicket(T4534Bean.getNum_ticket());
								BillStore_.setModo("1");
								BillStore_.setContenido(xmlCloud);
								BillStore_.setCorrelativo(T4534Bean.getNum_correl_ticket());
								BillStore_.setUsuarioModificador(T4534Bean.getCod_usumodif());
								t4243DAOSpring.insert_ticket_CPE(BillStore_);
								*/
							}else{
	
								//PAS20181U210300097 OBS
								if ( evaluarFirmaGEM(doc, parser, t4536bean.getFirma()) ){
								
									T4534Bean T4534Bean = t4534DAO.findByPK(numRuc, numSerieCpe, codCpe, numCpe);
									String arrayZip = nombreXml;
			
									BillStore BillStore_ = new BillStore();
									BillStore_.setTicket(T4534Bean.getNum_ticket());
									BillStore_.setModo("1");
									BillStore_.setContenido(zipBytes(arrayZip, bytes));
									BillStore_.setCorrelativo(T4534Bean.getNum_correl_ticket());
									BillStore_.setUsuarioModificador(T4534Bean.getCod_usumodif());
			
									log.debug(">>Grabando el xml validado por la firma");
									t4243DAOSpring.insert_ticket_CPE(BillStore_);
									
									t4243Bean = new T4243Bean();
									t4243Bean.setArc_zip(zipBytes(arrayZip, bytes));
								}
								
							}
						
						} catch (Exception e) {
							log.debug(">>Exception--" + e.getMessage());
							e.fillInStackTrace();
							throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
						}
					}

				}

			}else{
				
				try {
					log.debug("inicio llamada directa - Nuevo");
					byte[] xmlCloud = null;
					
					T4283Bean buscarT4283 = new T4283Bean();
					buscarT4283.setNum_ruc(numRuc);
					buscarT4283.setCod_cpe(codCpe);
					buscarT4283.setNum_serie_cpe(numSerieCpe);
					buscarT4283.setNum_cpe(numCpe);
					buscarT4283.setCod_rubro(IND_RUBRO_CLOUD_GUID); 
					
					if(log.isDebugEnabled()) log.debug("buscarT4283 ===>" + buscarT4283.toString());
					
					T4283Bean rubro = t4283DAO.findRubroCabecera_ByRUC_codCPE_Serie_CPE_Rubro(buscarT4283);
					String guid = "";
					
					if(log.isDebugEnabled()) log.debug("Despues de Consulta");
					
					if (rubro != null) {
						guid = rubro.getDes_detalle_rubro();
						if(log.isDebugEnabled()) log.debug("guid ===>" + guid);
					}
					else
					{
						if(log.isDebugEnabled()) log.debug("rubro es null ===>no se encuentra guid en la tabla t4283rubroscpe");
						//throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					
					
					log.debug("t4536bean==null-Consultando procesarComprobanteNubev2_0 buscarArchivoGemConGrabado");
					xmlCloud = this.procesarComprobanteNubev2_0(guid);
					log.debug("t4536bean==null-FIN-Consultando procesarComprobanteNubev2_0 buscarArchivoGemConGrabado");
					
					if(log.isDebugEnabled()) log.debug("xmlCloud ===>" + xmlCloud);
					
					if (xmlCloud.length>0) {
						t4243Bean = new T4243Bean();
					//	byte[] bytesTmp = xmlCloud.getBytes("ISO-8859-1");
						String nombreZip = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe+".xml";
						//t4243Bean.setArc_xml(xmlCloud);
						try {
							//t4243Bean.setArc_zip(zipBytes( nombreZip, xmlCloud ));
							t4243Bean.setArc_zip( xmlCloud );
						} catch (Exception e) {
							e.printStackTrace();
							throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
						}
					}
					else
					{
						if(log.isDebugEnabled()) log.debug("xmlCloud.length es 0 ===>no se encuentra comprobante en nube");
						throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					}
				 }
				} catch (Exception e) {
					log.debug(e.getMessage());
					throw new  ServiceException(this,"El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
				}
	    		
	    	}
		}
		

		return t4243Bean;
	}
	
	private String formatNombreXml(String nombreXml, String numRuc, String codCpe, String numSerieCpe, Integer numCpe){
		
		if(nombreXml == null || "".equals(nombreXml) ){
			nombreXml = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe+".xml";
		}else{
			nombreXml = nombreXml.replace(".zip", ".xml");
		}
		
		return nombreXml; 
	}
	
	public boolean evaluarFirmaGEM(Document doc, ComprobanteParserBean parser, String firma) {
		
		if(log.isDebugEnabled()) log.debug("========================== evaluarFirmaGEM ===========================");
		
		boolean result = false;		
    	
    	try {
    		Map<String,String> params = this.obtenerDatosComprobante(doc, parser.getXPath());
	    	
	    	/** verificar contra documento reportado a SUNAT. */	    	
	    	String queryXpath = "/sunat:"+ doc.getDocumentElement().getNodeName()+ "/ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/ds:Signature/ds:SignatureValue" ; 
	    	String signatureValueDoc = parser.getXPath().evaluate(queryXpath, doc, XPathConstants.STRING).toString(); 
			
	    	String signatureValueSUNAT = firma;
	    	
	    	//if(log.isDebugEnabled())log.debug(">> XML de SUNAT :: Firma del documento cargado: " + signatureValueDoc.trim() );
	    	//if(log.isDebugEnabled())log.debug(">> XML de SUNAT :: Firma del documento de bd sunat: " + signatureValueSUNAT.trim() );
	    	if(log.isDebugEnabled())log.debug(">> XML de SUNAT :: Resultado comparacion: " + signatureValueDoc.trim().equals(signatureValueSUNAT.trim()) ); 
	    	
	    	 if(signatureValueDoc.trim().equals(signatureValueSUNAT.trim()) ){
	    		 result = true;
	    	 }
			
		}catch(ServiceException e){				 
			log.error(e,e);
			result = false;
		}catch (Exception e) {			 
			log.error(e,e) ; 
			e.printStackTrace();
			result = false; 
		}
    	
    	return result;
	}
	
	private T6575ArchPerBean buscarArchivoPerceptionGEM(String numRuc, String codCpe, String serieCpe, Integer numCpe){
		
		T6575ArchPerBean t6575 = t6575Dao.selectByDocument(numRuc, codCpe, serieCpe, numCpe);
				
		if( null == t6575 ){
			if(log.isDebugEnabled()) log.debug("================== historicos PerceptionGEM ====================");
			
			T6571PercepcionBean t4571Bean_ = t6571Dao.buscarPorPk(numRuc, codCpe, serieCpe, numCpe);
			
			if(null != t4571Bean_){
			
				t6575 = t6575HistDao.selectByDocument( t4571Bean_.getNumTicket()-Long.valueOf(1) );
			
				if(null == t6575){
					
					if(log.isDebugEnabled()) log.debug("Consultando cloud - get xml");
					String xmlCloud = "";
					try {
						xmlCloud = this.PortalGetCPEResponse( numRuc, codCpe, serieCpe, numCpe );
						
					} catch (AccessTokenException e) {
						log.error("Error AccessTokenException :"+e);
						throw new ServiceException(this, "El documento con número de serie " + serieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					} catch (PortalSystemException e) {
						log.error("Error PortalSystemException :"+e);
						throw new ServiceException(this, "El documento con número de serie " + serieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					}
					
					if(log.isDebugEnabled()) log.debug("xmlCloud ===>"+xmlCloud);
					
					if(xmlCloud != null && !"".equals(xmlCloud)){
						t6575 = new T6575ArchPerBean();
						t6575.setArc_xml(xmlCloud);
					}
					
				}
			}
			
		}
		return t6575;
	}

	private T6576ArchRetBean buscarArchivoRetentionGEM(String numRuc, String codCpe, String serieCpe, Integer numCpe){
		
		T6576ArchRetBean t6576 = t6576Dao.selectByDocument(numRuc, codCpe, serieCpe, numCpe);
				
		if( null == t6576 ){
			if(log.isDebugEnabled()) log.debug("================== historicos RetentionGEM ====================");
						
			T6573RetencionBean t4573Bean_ = t6573Dao.buscarPorPk(numRuc, codCpe, serieCpe, numCpe);

			if(null != t4573Bean_){
			
				t6576 = t6576HistDao.selectByDocument( t4573Bean_.getNumTicket()-Long.valueOf(1) );
				
				if(null == t6576){
					
					if(log.isDebugEnabled()) log.debug("Consultando cloud - get xml");
					String xmlCloud = "";
					try {
						xmlCloud = this.PortalGetCPEResponse( numRuc, codCpe, serieCpe, numCpe );
					} catch (AccessTokenException e) {
						log.error("Error AccessTokenException :"+e);
						throw new ServiceException(this, "El documento con número de serie " + serieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					} catch (PortalSystemException e) {
						log.error("Error PortalSystemException :"+e);
						throw new ServiceException(this, "El documento con número de serie " + serieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					}
					
					if(log.isDebugEnabled()) log.debug("xmlCloud ===>"+xmlCloud);
					
					if(xmlCloud != null && !"".equals(xmlCloud)){
						t6576 = new T6576ArchRetBean();
						t6576.setArc_xml(xmlCloud);
					}
				}
				
			}
			
		}		
		return t6576;
	}
	
	private T6464ArcGreBean buscarArchivoDespatchAdviceGEM(String numRuc, String codCpe, String serieCpe, Integer numCpe){
				
		T6464ArcGreBean t6464 = t6464Dao.selectDoc(numRuc, codCpe, serieCpe, numCpe);
		
		if( null == t6464 ){
								
					if(log.isDebugEnabled()) log.debug("Consultando cloud - get xml");
					String xmlCloud = "";
					try {
						xmlCloud = this.PortalGetCPEResponse( numRuc, codCpe, serieCpe, numCpe );
					} catch (AccessTokenException e) {
						log.error("Error AccessTokenException :"+e);
						throw new ServiceException(this, "El documento con número de serie " + serieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					} catch (PortalSystemException e) {
						log.error("Error PortalSystemException :"+e);
						throw new ServiceException(this, "El documento con número de serie " + serieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
					}
					
					if(log.isDebugEnabled()) log.debug("xmlCloud ===>"+xmlCloud);
					
					if(xmlCloud != null && !"".equals(xmlCloud)){
						t6464 = new T6464ArcGreBean();
						t6464.setArc_xml(xmlCloud);
					}
			
		}
		
		return t6464;
	}
	
	
	
	//PAS20211U210700133
	private byte[] procesarComprobanteNubev2_0(String guid) throws Exception {
		byte[] resultado = null;
		if (log.isDebugEnabled()) log.debug("procesarComprobanteNubev2_0 : guid");

		try {
			
			log.info(guid + " CloudService - inicio cloudComprobanteService2_0");
			
			synchronized (CloudComprobanteStatic.class) {
				if (CloudComprobanteStatic.getComprobanteService() == null) {
					log.info(guid + " CloudComprobanteStatic - creacion cloudComprobanteServiceImpl");
					CloudComprobanteServiceImpl cloudComprobanteServiceImpl = new CloudComprobanteServiceImpl();
					cloudComprobanteServiceImpl.setDaoT01(t01DAO);
					cloudComprobanteServiceImpl.inicializarVariablesConsultasCloud();
					CloudComprobanteStatic.setComprobanteService(cloudComprobanteServiceImpl);
				} else {
					if (log.isDebugEnabled()) log.debug(guid + " CloudComprobanteStatic.getComprobanteService().esVigenteTokenCpes() = true");
				}
			}
			log.debug("CloudService - respCloudConsulta");
			byte[] respCloudConsulta = CloudComprobanteStatic.getComprobanteService().consultarComprobanteCloudv2_0(guid);
			
			
			
			if (respCloudConsulta.length > 0) {
				String msmError = guid + "-Se encontro el comprobante en la nube";
				log.info(msmError);
				resultado = respCloudConsulta;
			}else{
				String msmError = guid + "-No se encontro el comprobante en la nube";
				log.info(msmError);
				resultado = null;
				throw new Exception(msmError);
			}

		}catch (Exception e) {
			log.debug("error procesarComprobanteNubev2_0");
			log.debug(e.getMessage());
			throw new ServiceException(this, "Se presento un error al consumir el servicio de MS.");
		}
		
		log.info(guid + " CloudService - final cloudComprobanteService2_0");

		return resultado;
	}
	
	//PAS20191U210100231
	private String procesarComprobanteNube(String numRuc, String codCpe, String numSerieCpe, Integer numCpe) throws Exception {
		
		if (log.isDebugEnabled()) log.debug("procesarComprobanteNube : cpeid = numRuc + \"-\" + codCpe + \"-\" + numSerieCpe + \"-\" + numCpe");
		String cpeid = numRuc + "-" + codCpe + "-" + numSerieCpe + "-" + numCpe;

		String response = "";
		try {
			
			log.info(cpeid + " CloudService - inicio cloudComprobanteService");
			
			synchronized (CloudComprobanteStatic.class) {
				if (CloudComprobanteStatic.getComprobanteService() == null || !CloudComprobanteStatic.getComprobanteService().esVigenteTokenCustodia()) {
					log.info(cpeid + " CloudComprobanteStatic - creacion cloudComprobanteServiceImpl");
					CloudComprobanteServiceImpl cloudComprobanteServiceImpl = new CloudComprobanteServiceImpl();
					cloudComprobanteServiceImpl.setDaoT01(t01DAO);
					cloudComprobanteServiceImpl.inicializarVariablesCustodia();
					CloudComprobanteStatic.setComprobanteService(cloudComprobanteServiceImpl);
				} else {
					if (log.isDebugEnabled()) log.debug(cpeid + " CloudComprobanteStatic.getComprobanteService().esVigenteTokenCpes() = true");
				}
			}
			
			// [1]-correcto [-1]-no encontro [-2]-Otro error
			String[] respCloudConsulta = CloudComprobanteStatic.getComprobanteService().consultarComprobanteCloud(cpeid);
			
			if (respCloudConsulta[0].equals("-2")) {
				String msmError = cpeid + " Se presentó un error al momento de consultar. Vuelva a intentar en unos minutos.";
				log.info(msmError);
				throw new Exception(msmError);
			}else if (respCloudConsulta[0].equals("1")){
				response = respCloudConsulta[2];
			}else if (respCloudConsulta[0].equals("-1")){
				String msmError = cpeid + " No se encontro el comprobante en la nube.";
				log.info(msmError);
				throw new Exception(msmError);
			}

		}catch (Exception e) {
			log.debug("error procesarComprobanteNube");
			throw new ServiceException(this, "Se presento un error al consumir el servicio de MS.");
		}
		
		log.info(cpeid + " CloudService - final cloudComprobanteService");
		log.info("response:"+response);
		
		return response;
	}

	//Llamadas a nube
	public String PortalGetCPEResponse(String numRuc, String codCpe, String numSerieCpe, Integer numCpe) throws AccessTokenException, PortalSystemException {
		
		if (log.isDebugEnabled()) log.debug("Ini PortalGetCPEResponse");
		
		String response = "";
		try {

			String cpeId = numRuc+"-"+codCpe+"-"+numSerieCpe+"-"+numCpe;//"20338570041-03-BB99-146"
			String token = this.getAccessToken(false, numSerieCpe, numCpe);
			
			if (log.isDebugEnabled()) log.debug("token = " + token);			
			response = this.getServiceXmlFile(token, cpeId, "document", numSerieCpe, numCpe);	
			
		} catch(AccessTokenException e) { 
			log.error("Error AccessTokenException :"+e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		} catch(PortalSystemException e) { 
			log.error("Error PortalSystemException :"+e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		} catch (Exception e) {
			log.error("Error Exception :"+e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		}
		
		if (log.isDebugEnabled()) log.debug("Fin PortalGetCPEResponse");
		
		return response;	
	}
	
	private String getAccessToken(boolean esOperacion, String numSerieCpe, Integer numCpe) throws AccessTokenException {
		
		String tokenRequestUrl =  t01DAO.findByArgumento(UConstante.NUM_PARAMETRO_API_CPES_NUBE,"01").substring(29, 130).trim(); 
		String clientId_query_xml = t01DAO.findByArgumento(UConstante.NUM_PARAMETRO_API_CPES_NUBE,"09").substring(29, 130).trim();
    	String clientSecret_query_xml = t01DAO.findByArgumento(UConstante.NUM_PARAMETRO_API_CPES_NUBE,"10").substring(29, 130).trim();
    	
    	if (log.isDebugEnabled()) log.debug("tokenRequestUrl==" + tokenRequestUrl + "==");
    	if (log.isDebugEnabled()) log.debug("clientId_query_xml==" + clientId_query_xml + "==");
    	if (log.isDebugEnabled()) log.debug("clientSecret_query_xml==" + clientSecret_query_xml + "==");

		OAuthClientRequest request = null;
		try {

			if(!esOperacion) {
				request = OAuthClientRequest.tokenLocation(tokenRequestUrl).setGrantType(GrantType.CLIENT_CREDENTIALS)
						.setClientId(clientId_query_xml).setClientSecret(clientSecret_query_xml).setParameter("resource", clientId_query_xml)
						.buildBodyMessage();
			}
			
		} catch (OAuthSystemException e) {
			log.error("Error getAccessToken :" + e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		} catch (Exception e) {;
			log.error("Error getAccessToken Exception :" + e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		}
		
		OAuthJSONAzureAccessToken azureAccessToken = OAuthJSONAzureAccessToken.getInstance();
		
		return azureAccessToken.getAccessToken(request);
	
	}
	
	
	private String getServiceXmlFile(String token, String cpeid, String tipoDocument, String numSerieCpe, Integer numCpe)  throws PortalSystemException {

		PortalClient client = new PortalClient(new URLConnectionPortalClient());
		
		String resourceUrlCpesFiles = t01DAO.findByArgumento(UConstante.NUM_PARAMETRO_API_CPES_NUBE,"06").substring(29, 130).trim() + "/:" + Portal.PORTAL_CPE_ID;
		
		if (log.isDebugEnabled()) log.debug("resourceUrlCpes==" + resourceUrlCpesFiles + "==");

		PortalClientRequest request = null;
		try {
						
			request = CpePortalClientRequest.resourceLocation(resourceUrlCpesFiles.replace(":cpeid", cpeid)).setParameter("type", tipoDocument)
                    	.buildQueryMessage();

			request.addAuthorizationHeader(token);
			
		} catch (PortalSystemException e) {
			log.error("Error al configurar el cliente Portal = ", e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		}

		PortalQueryXmlFileResponse portalCPEResponse = null;
		try {
			
			portalCPEResponse = client.callService(request, Portal.HttpMethod.GET, PortalQueryXmlFileResponse.class);

            String respuesta = portalCPEResponse.getBody();

            return respuesta;

		} catch (PortalSystemException e) {
			log.error("Error PortalSystemException : ", e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		} catch (PortalProblemException e) {
			log.error("Error PortalProblemException : ", e);
			throw new ServiceException(this, "El documento con número de serie " + numSerieCpe + "-" + numCpe + " no ha sido informado a SUNAT.");
		}
		
	}
	//
	
	private String obtenerContenidoNodo(Element doc, String nameNode){

		NodeList nodos = doc.getChildNodes() ;
		Node nodo = null ;
		String tmpNombreNodo = "" ; 
		String valorNodo = "" ; 

		for(int i =0 ;i<nodos.getLength(); i++){
			nodo = nodos.item(i);    					
			if(  (nodo.getNodeName().trim()).contains(nameNode)  ){				 
				Element el = (Element)nodo;
				valorNodo = el.getFirstChild().getNodeValue();
				break ; 
			}
		}

		if(log.isDebugEnabled()) log.debug(">> "+nodo+ " - "+tmpNombreNodo+" - "+ valorNodo);
		return valorNodo ;

	}
	
	public T4241DAO getT4241DAO() {
		return t4241DAO;
	}

	public void setT4241DAO(T4241DAO t4241dao) {
		t4241DAO = t4241dao;
	}
        
	public T10194DAO getT10194DAO() {
		return t10194DAO;
	}

	public void setT10194DAO(T10194DAO t10194dao) {
		t10194DAO = t10194dao;
	}

	public T4243DAO getT4243DAO() {
		return t4243DAO;
	}

	public void setT4243DAO(T4243DAO t4243dao) {
		t4243DAO = t4243dao;
	}

	public T3639DAO getT3639DAO() {
		return t3639DAO;
	}

	public void setT3639DAO(T3639DAO t3639dao) {
		t3639DAO = t3639dao;
	}

	public ProcesaArchivoComprobanteService getArchivoService() {
		return archivoService;
	}

	public DigitalCertificateService getCertificacionDigital() {
		return certificacionDigital;
	}

	public void setArchivoService(ProcesaArchivoComprobanteService archivoService) {
		this.archivoService = archivoService;
	}

	public void setCertificacionDigital(DigitalCertificateService certificacionDigital) {
		this.certificacionDigital = certificacionDigital;
	}

	public T6575ArchPerDAO getT6575Dao() {
		return t6575Dao;
	}

	public void setT6575Dao(T6575ArchPerDAO t6575Dao) {
		this.t6575Dao = t6575Dao;
	}

	public T6571PercepcionSelectDAO getT6571Dao() {
		return t6571Dao;
	}

	public void setT6571Dao(T6571PercepcionSelectDAO t6571Dao) {
		this.t6571Dao = t6571Dao;
	}

	public T6576ArchRetDAO getT6576Dao() {
		return t6576Dao;
	}

	public void setT6576Dao(T6576ArchRetDAO t6576Dao) {
		this.t6576Dao = t6576Dao;
	}

	public T6576DAO getT6576HistDao() {
		return t6576HistDao;
	}

	public void setT6576HistDao(T6576DAO t6576HistDao) {
		this.t6576HistDao = t6576HistDao;
	}

	public T6575DAO getT6575HistDao() {
		return t6575HistDao;
	}

	public void setT6575HistDao(T6575DAO t6575HistDao) {
		this.t6575HistDao = t6575HistDao;
	}

	public T6573RetencionSelectDAO getT6573Dao() {
		return t6573Dao;
	}

	public void setT6573Dao(T6573RetencionSelectDAO t6573Dao) {
		this.t6573Dao = t6573Dao;
	}

	public T6464ArcGreDAO getT6464Dao() {
		return t6464Dao;
	}

	public void setT6464Dao(T6464ArcGreDAO t6464Dao) {
		this.t6464Dao = t6464Dao;
	}

	public T6460CabGreDAO getT6460Dao() {
		return t6460Dao;
	}

	public void setT6460Dao(T6460CabGreDAO t6460Dao) {
		this.t6460Dao = t6460Dao;
	}

	public T4536DAO getDaoT4536() {
		return daoT4536;
	}

	public void setDaoT4536(T4536DAO daoT4536) {
		this.daoT4536 = daoT4536;
	}

	public T4536DAO getDaoT4536Hist() {
		return daoT4536Hist;
	}

	public void setDaoT4536Hist(T4536DAO daoT4536Hist) {
		this.daoT4536Hist = daoT4536Hist;
	}

	public T4534DAO getT4534DAO() {
		return t4534DAO;
	}

	public void setT4534DAO(T4534DAO t4534dao) {
		t4534DAO = t4534dao;
	}
	
	public T10204DAO getT10204DAO() {
		return t10204DAO;
	}

	public void setT10204DAO(T10204DAO t10204dao) {
		t10204DAO = t10204dao;
	}
	
	public T10209DAO getT10209DAO() {
		return t10209DAO;
	}

	public void setT10209DAO(T10209DAO t10209dao) {
		t10209DAO = t10209dao;
	}

	// PAS20211U210700133
	public T4283DAO getT4283DAO() {
		return t4283DAO;
	}

	public void setT4283DAO(T4283DAO t4283dao) {
		t4283DAO = t4283dao;
	}
}
