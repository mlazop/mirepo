package pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.impl;
import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdcpe.*;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdcpe.dao.*;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdparametros.domain.Contribuyentes;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdparametros.domain.PersonasNaturales;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdparametros.domain.dao.ContribuyenteRepository;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdparametros.domain.dao.PersonasNaturalesRepository;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdparametros.domain.dao.UbigeoRepository;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdrecauda.T01param;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdrecauda.T01paramPK;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdrecauda.dao.jpa.ParametriaRepositoryImpl;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.compago.cloud.svc.impl.ConsultaCPEServiceImpl;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.compago.cloud.util.CloudUtilBean;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.domain.*;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.ComprobantesRepository;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.ParametroRepository;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.bean.ParametrosSer;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.bean.TiposComprobanteRelacionado;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.bean.TiposDocumento;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.bean.TiposMoneda;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.bean.TiposNota;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.repository.bean.TiposUnidades;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.Constantes;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.EnumTipoComp;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.PropertiesBean;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.RubrosEnum;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.bean.ComprobanteUtilBean;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.exceptions.ErrorMessage;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.exceptions.UnprocessableEntityException;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.util.vo.UbigeoVO;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.ws.dto.ArchivoNfsResponseDTO;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.ws.dto.ArchivoRequestDTO;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.ws.dto.ArchivoResponseDTO;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.ws.dto.ComprobanteIndividualRequestDTO;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.ws.dto.ComprobanteMasivaRequestDTO;
import pe.gob.sunat.framework.spring.util.lang.Numero;
import pe.gob.sunat.tecnologiams.arquitectura.framework.core.util.UtilLog;
import pe.gob.sunat.tecnologiams.arquitectura.framework.common.util.ConstantesUtils;
import javax.inject.Inject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import java.util.Base64;

import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdcpehist.dao.RubrosHistRepository;

import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.json.Json;
import javax.json.JsonValue;




//import pe.gob.sunat.framework.util.lang.Numero;

public class ComprobantesRepositoryImpl implements ComprobantesRepository {
    @Inject
    private ComprobanteRepository comprobanteRepository;
    @Inject
    private DetComprobanteRepository detComprobanteRepository;
    @Inject
    private ContribuyenteRepository contribuyenteRepository;
    @Inject
    private UbigeoRepository ubigeoRepository;

    @Inject
    private PercepcionRepository percepcionRepository;
    
    @Inject
    private RubrosRepository rubrosRepository;
    
    @Inject
    private RubrosHistRepository rubrosRepositoryhist;
    
    @Inject
    private DocRelacionadoRepository docRelacionadoRepository;
    @Inject
    private VehiculoRepository vehiculoRepository;
    @Inject
    private ComprobanteXMLRepository comprobanteXMLRepository;
    @Inject
    private CompFestoreRepository compFestoreRepository;
    @Inject
    private DetResumBoletaRepository detResumBoletaRepository;
    @Inject
    private DetPeiRepository detPeiRepository;
    @Inject
    private ParametroRepository parametroRepository;

    @Inject
    private DireccionRepository direccionRepository;
    @Inject
    private UtilLog utilLog;
    @Inject
    private PropertiesBean properties;
    @Inject
    PersonasNaturalesRepository personasNaturalesRepository;
    private ConsultaCPEServiceImpl consultaCPEService;
  	
    protected static final String[] COD_PROCEDENCIAS_PORTAL= { "1", "4","6","7","8"};
    protected static final String[] COD_PROCEDENCIAS_GEM= { "2", "3","5"};
    protected static final String[] COD_NOTA_DEBITO_GEM= { "01", "02","03","04","05"};
	
    protected static final String[] LIST_AGRUP_REL_FACBOL_CATALOGO1 = { Constantes.COD_RELACION_01, Constantes.COD_RELACION_02,Constantes.COD_RELACION_03 };
	protected static final String[] LIST_AGRUP_REL_FACBOL_CATALOGO12 = { Constantes.COD_RELACION_04 };
	protected static final String[] LIST_AGRUP_REL_NOTAS_CATALOGO1 = { Constantes.COD_RELACION_01, Constantes.COD_RELACION_02,Constantes.COD_RELACION_05 };
	
  	public ComprobantesRepositoryImpl() {
  		this.consultaCPEService = new ConsultaCPEServiceImpl();
  	}


    @Override
    public List<Comprobantes> obtenerComprobanteIndividual(ComprobanteIndividualRequestDTO request, Date fecEmision) throws Exception {

        List<Comprobantes> comprobantes = new ArrayList<>();
        Comprobantes comprobanteindivual = new Comprobantes();
        ParametrosSer parametros = parametroRepository.listarParametros();

        //obtener parametros
        DatosEmisor datosEmisor = new DatosEmisor();
        DatosReceptor datosReceptor = new DatosReceptor();
        ComprobantePK comprobantePk = new ComprobantePK();
        comprobantePk.setCodCpe(request.getCodCpe());
        String rucHeader = request.getNumRucHeader();
        String tipoFiltro = request.getCodFiltroCpe();
        boolean noEsResuamen=true;
        boolean siDocRel=false;
        String codDocRel=Constantes.COD_CPE_NINGUNO;
        if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)||request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)||
        		request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA)){ 
        noEsResuamen=false;
        }
        
        if(request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_BOLETA)||request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)||
           request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_BOLETA)||request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)){
            codDocRel=Constantes.COD_CPE_BOLETA;
        }
        if(request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_FACTURA)||request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_FACTURA)){
            codDocRel=Constantes.COD_CPE_FACTURA;
        }
        
        if (request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_BOLETA) 
        		|| request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_FACTURA) 
        		|| request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_NOTA_CREDITO);
        }else if (request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_FACTURA) 
        		|| request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_BOLETA) 
        		|| request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_NOTA_DEBITO);
        }else if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_BOLETA);
        }
        
        comprobantePk.setNumCpe(request.getNumCpe().intValue());
        comprobantePk.setNumRuc(request.getNumRucEmisor());
        comprobantePk.setNumSerieCpe(request.getNumSerieCpe());
        
        if (noEsResuamen) {
        	Comprobante comprobante = Optional
                    .ofNullable(comprobanteRepository.obtenerComprobante(comprobantePk, tipoFiltro, rucHeader))
                    .orElse(null);
        	if (Objects.nonNull(comprobante)) {
                    
                    //ontener comprobante relacionado si es nota de credito o debito
                    DocumentoRelacionadoPK documentoRelacionadoPK = new DocumentoRelacionadoPK();
	            documentoRelacionadoPK.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
	            documentoRelacionadoPK.setCodCpe(comprobante.getComprobantePK().getCodCpe());
	            documentoRelacionadoPK.setNumRuc(comprobante.getComprobantePK().getNumRuc());
	            documentoRelacionadoPK.setNumCpe(comprobante.getComprobantePK().getNumCpe());
                    List<DocumentoRelacionado> documentoRelacionados = Optional
	                    .ofNullable(docRelacionadoRepository.obtenerDocumentosRelacionados(documentoRelacionadoPK))
	                    .orElse(null);
                    if(request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_FACTURA)||request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_FACTURA) ||
                       request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_BOLETA)||request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_BOLETA)){
                        for (DocumentoRelacionado documentoRelacionado : documentoRelacionados) {
                            if(documentoRelacionado.getDocumentoRelacionadoPK().getCodDocRel().equals(codDocRel)){
                                siDocRel=true;
                                break;
                            }
                        }
                    }
                    if((codDocRel.equals(Constantes.COD_CPE_FACTURA)||codDocRel.equals(Constantes.COD_CPE_BOLETA))&&siDocRel){
                    //obtener detalle
	            DetalleComprobantePK detalleComprobantePK = new DetalleComprobantePK();
	            detalleComprobantePK.setCodCpe(comprobante.getComprobantePK().getCodCpe());
	            detalleComprobantePK.setNumCpe(comprobante.getComprobantePK().getNumCpe());
	            detalleComprobantePK.setNumRuc(request.getNumRucEmisor());
	            detalleComprobantePK.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
	
	            //obtener rubros
	            RubrosPK rubrosPk = new RubrosPK();
	            rubrosPk.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
	            rubrosPk.setCodCpe(comprobante.getComprobantePK().getCodCpe());
	            rubrosPk.setNumRuc(comprobante.getComprobantePK().getNumRuc());
	            rubrosPk.setNumCpe(comprobante.getComprobantePK().getNumCpe());
	            List<Rubros> rubros = Optional
	                    .ofNullable(rubrosRepository.obtenerRubros(rubrosPk))
	                    .orElse(null);
                    if(rubros==null || rubros.isEmpty()){
                    rubros = Optional
                            .ofNullable(rubrosRepositoryhist.obtenerRubros(rubrosPk))
                            .orElse(null);
                    }
	            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "RESULTADO RUBROS" + rubros);
	
	            List<DetalleComprobante> detalleComprobante = Optional
	                    .ofNullable(detComprobanteRepository.obtenerDetalleComprobante(detalleComprobantePK))
	                    .orElse(null);
	            //obtener el xml
	            Document document = obtenerValorNodoXml(comprobante.getComprobantePK().getNumRuc()
	            		, comprobante.getComprobantePK().getCodCpe(), comprobante.getComprobantePK().getNumSerieCpe()
	            		, String.valueOf(comprobante.getComprobantePK().getNumCpe())
	            		, request.getCodCpe(), Constantes.COD_TIPO_CONSULTA_INDIVIDUAL);
	            
	            if(document==null){
	            	ArchivoRequestDTO archivoRequestDTO = new ArchivoRequestDTO();
	            		
	            	archivoRequestDTO.setCodCpe(comprobante.getComprobantePK().getCodCpe());
	            	archivoRequestDTO.setNumRucEmisor(comprobante.getComprobantePK().getNumRuc());
	            	archivoRequestDTO.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
	            	archivoRequestDTO.setNumCpe(request.getNumCpe());
	            	archivoRequestDTO.setCodCpeOri(request.getCodCpe());
	            	 document = recuperaComprobanteXmlDocNube(archivoRequestDTO);
	            }
	            
	            
	            if(comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_FACTURA) 
	            		|| comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_BOLETA))
	            {
	                comprobanteindivual=llenarDatosFacturaBoleta(comprobante,detalleComprobante,rubros,document,parametros,fecEmision,documentoRelacionados);
	            }
	            if(comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO) || comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO))
	            {
	                comprobanteindivual=llenarDatosNotas(comprobante,detalleComprobante,rubros,document,parametros,fecEmision,documentoRelacionados);
	
	            }
	            comprobantes.add(comprobanteindivual);
	            
               }else if(codDocRel.equals(Constantes.COD_CPE_NINGUNO)){
            	   
	                //obtener detalle
		            DetalleComprobantePK detalleComprobantePK = new DetalleComprobantePK();
		            detalleComprobantePK.setCodCpe(comprobante.getComprobantePK().getCodCpe());
		            detalleComprobantePK.setNumCpe(comprobante.getComprobantePK().getNumCpe());
		            detalleComprobantePK.setNumRuc(request.getNumRucEmisor());
		            detalleComprobantePK.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
		
		            //obtener rubros
		            RubrosPK rubrosPk = new RubrosPK();
		            rubrosPk.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
		            rubrosPk.setCodCpe(comprobante.getComprobantePK().getCodCpe());
		            rubrosPk.setNumRuc(comprobante.getComprobantePK().getNumRuc());
		            rubrosPk.setNumCpe(comprobante.getComprobantePK().getNumCpe());
		            List<Rubros> rubros = Optional
		                    .ofNullable(rubrosRepository.obtenerRubros(rubrosPk))
		                    .orElse(null);
	                    if(rubros==null || rubros.isEmpty()){
	                    rubros = Optional
	                            .ofNullable(rubrosRepositoryhist.obtenerRubros(rubrosPk))
	                            .orElse(null);
	                    }
		            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "RESULTADO RUBROS" + rubros);
		
		            List<DetalleComprobante> detalleComprobante = Optional
		                    .ofNullable(detComprobanteRepository.obtenerDetalleComprobante(detalleComprobantePK))
		                    .orElse(null);
		            //obtener el xml
		            Document document = obtenerValorNodoXml(comprobante.getComprobantePK().getNumRuc()
		            		, comprobante.getComprobantePK().getCodCpe(), comprobante.getComprobantePK().getNumSerieCpe()
		            		, String.valueOf(comprobante.getComprobantePK().getNumCpe())
		            		, request.getCodCpe(), Constantes.COD_TIPO_CONSULTA_INDIVIDUAL);
		            
		            if(comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_FACTURA) 
		            		|| comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_BOLETA))
		            {
		            	if(document==null){
			            	ArchivoRequestDTO archivoRequestDTO = new ArchivoRequestDTO();
			            		
			            	archivoRequestDTO.setCodCpe(comprobante.getComprobantePK().getCodCpe());
			            	archivoRequestDTO.setNumRucEmisor(comprobante.getComprobantePK().getNumRuc());
			            	archivoRequestDTO.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
			            	archivoRequestDTO.setNumCpe(request.getNumCpe());
			            	archivoRequestDTO.setCodCpeOri(request.getCodCpe());
			            	 document = recuperaComprobanteXmlDocNube(archivoRequestDTO);
			            }
		                comprobanteindivual=llenarDatosFacturaBoleta(comprobante,detalleComprobante,rubros,document,parametros,fecEmision,documentoRelacionados);
		            }
		            if(comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO) || comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO))
		            {
		            	if(document==null){
			            	ArchivoRequestDTO archivoRequestDTO = new ArchivoRequestDTO();
			            		
			            	archivoRequestDTO.setCodCpe(comprobante.getComprobantePK().getCodCpe());
			            	archivoRequestDTO.setNumRucEmisor(comprobante.getComprobantePK().getNumRuc());
			            	archivoRequestDTO.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
			            	archivoRequestDTO.setNumCpe(request.getNumCpe());
			            	archivoRequestDTO.setCodCpeOri(request.getCodCpe());
			            	 document = recuperaComprobanteXmlDocNube(archivoRequestDTO);
			            }
		                comprobanteindivual=llenarDatosNotas(comprobante,detalleComprobante,rubros,document,parametros,fecEmision,documentoRelacionados);
		
		            }
		            if(comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_LIQUIDACION_COMPRA))
		            {
		                comprobanteindivual=llenarDatosLiquidacion(comprobante,detalleComprobante,rubros,document,parametros,fecEmision,documentoRelacionados);
		
		            }
		            comprobantes.add(comprobanteindivual);
                }                   
        	}else if (request.getCodCpe().equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)) {

                DetalleLiqPei detalleLiqPei = Optional
                        .ofNullable(detPeiRepository.obtenerLiquidacionPei(request.getNumRucEmisor(), request.getCodCpe(), request.getNumSerieCpe(), request.getNumCpe().toString()))
                        .orElse(null);
                if (Objects.nonNull(detalleLiqPei)) {

                    datosEmisor.setNumRuc(detalleLiqPei.getNumDocIdComp());
                    datosEmisor.setDesRazonSocialEmis(detalleLiqPei.getDesNombreComp());
                    datosEmisor.setDesNomComercialEmis("-");

                    datosReceptor.setCodDocIdeRecep(detalleLiqPei.getCodDocIdVend());
                    comprobanteindivual.setNumSerie(detalleLiqPei.getNumSerie());
                    datosReceptor.setNumDocIdeRecep(detalleLiqPei.getNumDocIdVend());
                    datosReceptor.setDesRazonSocialRecep(detalleLiqPei.getDesNombreVend());
                    datosReceptor.setDirDetCliente(detalleLiqPei.getDesDirVend());
                    datosReceptor.setDirDetVendedor(detalleLiqPei.getDesDirVend() + " " + detalleLiqPei.getDesDeptDir() + "-" + detalleLiqPei.getDesProvDir() + "-" + detalleLiqPei.getDesDistDir());
                    datosReceptor.setDersLugarOperacion(obtenerDatosDireccionPei(detalleLiqPei.getCodUbiComp()));
                    if (!detalleLiqPei.getCodDocIdVend().isEmpty() && !detalleLiqPei.getCodDocIdVend().equals("-")) {
                        comprobanteindivual.setDesTipoCpe(parametros.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(detalleLiqPei.getCodDocIdVend().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

                    } else {
                    		comprobanteindivual.setDesTipoCpe("-");
                    }
                    
                    if("00".endsWith(detalleLiqPei.getCodDocIdVend().trim()) || "0".endsWith(detalleLiqPei.getCodDocIdVend().trim())){
                		comprobanteindivual.setDesTipoCpe(parametros.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
                	}
                    
                    comprobanteindivual.setDesMoneda(parametros.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(detalleLiqPei.getCodMoneda())).findFirst().map(TiposMoneda::getDesMoneda).orElse("-"));
                    comprobanteindivual.setDesSimbolo(parametros.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(detalleLiqPei.getCodMoneda())).findFirst().map(TiposMoneda::getCodSimbolo).orElse("-"));
                    comprobanteindivual.setCodCpe(detalleLiqPei.getCodCdp());
                    comprobanteindivual.setNumCpe(Integer.parseInt(detalleLiqPei.getNumCdp()));
                    comprobanteindivual.setCodMoneda(detalleLiqPei.getCodMoneda());
                    comprobanteindivual.setFecEmision(detalleLiqPei.getFecEmisCdp());
                    comprobanteindivual.setCodTipTransaccion("");
                    comprobanteindivual.setIndProcedencia(Constantes.PROCEDENCIA_PEI);
                    if(detalleLiqPei.getCodTipoEnvio().equals(Constantes.COD_ESTADO_PEI_ACTIVO)){
                    comprobanteindivual.setIndEstadoCpe(Constantes.COD_ESTADO_ACTIVO);
                    }
                    if(detalleLiqPei.getCodTipoEnvio().equals(Constantes.COD_ESTADO_PEI_ANULADO)){
                    comprobanteindivual.setIndEstadoCpe(Constantes.COD_ESTADO_BAJA);
                    }
                    comprobanteindivual.setDesMtoTotalLetras(Numero.numerosALetras(detalleLiqPei.getMtoOpeNeto()) + " "
                            + ComprobanteUtilBean.getDescripcionMoneda(detalleLiqPei.getCodMoneda()));
                    comprobanteindivual.setDesObservacion("");
                    comprobanteindivual.setIndTituloGratuito( detalleLiqPei.getIndOpeGratu());


                    ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();

                    procedenciaMasiva.setMtoTotalValVentaGrabado(detalleLiqPei.getMtoOperGrav() != null ? detalleLiqPei.getMtoOperGrav() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoTotalValVentaInafecto(detalleLiqPei.getMtoOpeInaf() != null ? detalleLiqPei.getMtoOpeInaf() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoTotalValVentaExonerado(detalleLiqPei.getMtoOperExo() != null ? detalleLiqPei.getMtoOperExo() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoImporteTotal(detalleLiqPei.getMtoOpeNeto() != null ? detalleLiqPei.getMtoOpeNeto() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoTotalAnticipo(detalleLiqPei.getMtoAnticipo());
                    procedenciaMasiva.setMtoSumOtrosTributos(BigDecimal.ZERO);
                    procedenciaMasiva.setMtoSumIgvCredito(detalleLiqPei.getMtoRetIgv().add(procedenciaMasiva.getMtoSumOtrosTributos()));//IGV+Otros Tributos
                    procedenciaMasiva.setMtoSumIrRetencion(detalleLiqPei.getMtoRetIr());
                    procedenciaMasiva.setMtoTotalValVenta((detalleLiqPei.getMtoOperGrav() != null ? detalleLiqPei.getMtoOperGrav() : BigDecimal.ZERO).add(detalleLiqPei.getMtoOpeInaf() != null ? detalleLiqPei.getMtoOpeInaf() : BigDecimal.ZERO).add(detalleLiqPei.getMtoOperExo() != null ? detalleLiqPei.getMtoOperExo() : BigDecimal.ZERO));
                    procedenciaMasiva.setMtoTotalValVentaLiquidacion(procedenciaMasiva.getMtoTotalValVenta().add(detalleLiqPei.getMtoRetIgv()).add(procedenciaMasiva.getMtoSumOtrosTributos()));
                    procedenciaMasiva.setMtoRedondeo(BigDecimal.ZERO);
                    procedenciaMasiva.setMtoSumIGV(detalleLiqPei.getMtoRetIgv());
                    
                    
                    if(detalleLiqPei.getIndAdGoro().equals("1")){
                    List<InformacionItems> listItemsLista=new ArrayList<>();
                    InformacionItems info = new InformacionItems();
                    info.setCntItems(detalleLiqPei.getCntAdgoro());
                    info.setCodUnidadMedida(detalleLiqPei.getCodUniMed());
                    String desUnidad=parametros.getTiposUnidades().stream().filter(tiposUnidades -> tiposUnidades.getCodUnidad().equals(detalleLiqPei.getCodUniMed())).findFirst().map(TiposUnidades::getDesUnidad).orElse("-");
                    if(desUnidad!=null && desUnidad.length()>0){
                        String[] desUnidadArray= desUnidad.split("\\(");
                        String desUnidadPrimerResultado = desUnidadArray[0].trim();
                        info.setDesUnidadMedida(desUnidadPrimerResultado);
                    }          
                    info.setDesCodigo(detalleLiqPei.getDesCodDerMiner());
                    info.setDesItem(detalleLiqPei.getDesNomDerMiner());
                    info.setMtoValUnitario(detalleLiqPei.getMtoValUnir());
                    listItemsLista.add(info);
                    comprobanteindivual.setInformacionItems(listItemsLista);
                    }                                             
                    
                    comprobanteindivual.setProcedenciaMasiva(procedenciaMasiva);

                    List<VehiculoTransporte> vehiculosTransporte = new ArrayList<>();
                    VehiculoPK vehiculopk = new VehiculoPK();
                    vehiculopk.setNumSerieCpe(detalleLiqPei.getNumSerie());
                    vehiculopk.setCodCpe(detalleLiqPei.getCodCdp());
                    vehiculopk.setNumRuc(detalleLiqPei.getNumDocIdComp());
                    vehiculopk.setNumCpe(Integer.parseInt(detalleLiqPei.getNumCdp()));
                    List<Vehiculo> vehiculos = Optional
                            .ofNullable(vehiculoRepository.listarVehiculos(vehiculopk)).orElse(null);
                    if (Objects.nonNull(vehiculos)) {
                        for (Vehiculo vehiculo : vehiculos) {
                            VehiculoTransporte vehiculoTransporte = new VehiculoTransporte();
                            vehiculoTransporte.setPlaca(vehiculo.numPlaca);
                            vehiculosTransporte.add(vehiculoTransporte);
                        }
                        if (!vehiculosTransporte.isEmpty()) {
                            comprobanteindivual.setVehiculosTransporte(vehiculosTransporte);

                        }
                    }


                    DocumentoRelacionadoPK documentoRelacionadoPK = new DocumentoRelacionadoPK();
                    documentoRelacionadoPK.setNumSerieCpe(detalleLiqPei.getNumSerie());
                    documentoRelacionadoPK.setCodCpe(detalleLiqPei.getCodCdp());
                    documentoRelacionadoPK.setNumRuc(detalleLiqPei.getNumDocIdComp());
                    List<InformacionDocumentosRelacionados> informacionDocumentosRelacionados = new ArrayList<>();

                    List<DocumentoRelacionado> documentoRelacionados = Optional
                            .ofNullable(docRelacionadoRepository.obtenerDocumentosRelacionados(documentoRelacionadoPK))
                            .orElse(null);

                    for (DocumentoRelacionado documentoRel : documentoRelacionados) {
                    	
                    	String codAgrup = Arrays.asList(LIST_AGRUP_REL_FACBOL_CATALOGO12)
            	                .contains(documentoRel.getCodRelacion())
            	                ? Constantes.COD_AGRUP_REL_CATALOGO12:Constantes.COD_AGRUP_REL_CATALOGO1;

                        if (parametros.getTiposComprobanteRelacionado().stream()
            					.anyMatch(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
            							.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
            							&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))) {
            				
            				InformacionDocumentosRelacionados infdoc = new InformacionDocumentosRelacionados();
            		
            					infdoc.setCodCpeRel(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
            					infdoc.setNumDocRel(documentoRel.getDocumentoRelacionadoPK().getNumDocRel());
            					infdoc.setNumSerieDocRel(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel());
            					infdoc.setDesCpeRel(parametros.getTiposComprobanteRelacionado().stream()
            							.filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
            									.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
            									&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))
            							.findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
            					infdoc.setCodAgrupProc(codAgrup);
            					informacionDocumentosRelacionados.add(infdoc);
            				}
            			}

                    if (!informacionDocumentosRelacionados.isEmpty()) {
                        comprobanteindivual.setInformacionDocumentosRelacionados(informacionDocumentosRelacionados);

                    }
                    comprobanteindivual.setDatosEmisor(datosEmisor);
                    comprobanteindivual.setDatosReceptor(datosReceptor);
                    comprobanteindivual.setProcedenciaMasiva(procedenciaMasiva);

                    comprobantes.add(comprobanteindivual);
                }

            }
        	
        }else{
            if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA) 
            		|| request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO) 
            		|| request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)) {
                DetResumBoletaPK detResumBoletaPK = new DetResumBoletaPK();
                detResumBoletaPK.setCodCpe(comprobantePk.getCodCpe());
                detResumBoletaPK.setNumRuc(request.getNumRucEmisor());
                detResumBoletaPK.setNumSerieCpe(request.getNumSerieCpe());
                DetResumBoleta detResumBoletas = Optional
                        .ofNullable(detResumBoletaRepository.obtenerResumBoleta(detResumBoletaPK, request.getNumCpe().intValue(),request.getCodFiltroCpe(),request.getNumRucHeader()))
                        .orElse(null);
                if (Objects.nonNull(detResumBoletas)) {

                    Document document = obtenerValorNodoXml(detResumBoletas.getDetResumBoletaPK().getNumRuc()
                    		, detResumBoletas.getDetResumBoletaPK().getCodCpe()
                    		, detResumBoletas.getDetResumBoletaPK().getNumSerieCpe()
                    		, String.valueOf(detResumBoletas.getNumDesde())
                    		, request.getCodCpe(), Constantes.COD_TIPO_CONSULTA_INDIVIDUAL);
                    
                    if(document==null){
    	            	ArchivoRequestDTO archivoRequestDTO = new ArchivoRequestDTO();
    	            		
    	            	archivoRequestDTO.setCodCpe(detResumBoletas.getDetResumBoletaPK().getCodCpe());
    	            	archivoRequestDTO.setNumRucEmisor(detResumBoletas.getDetResumBoletaPK().getNumRuc());
    	            	archivoRequestDTO.setNumSerieCpe(detResumBoletas.getDetResumBoletaPK().getNumSerieCpe());
    	            	archivoRequestDTO.setNumCpe(request.getNumCpe());
    	            	archivoRequestDTO.setCodCpeOri(request.getCodCpe());        	
    	            	 document = recuperaComprobanteXmlDocNube(archivoRequestDTO);
    	            }

                    datosEmisor.setDesDirEmis(obtenerValorNodeDireccion(document,detResumBoletas.getDetResumBoletaPK().getCodCpe()));
                    String dirEstEmisor = obtenerValorNodeDirecEstEmisor(document);
                    if (!dirEstEmisor.isEmpty()) {
                        datosEmisor.setDesDirEstEmis(dirEstEmisor);

                    }

                    datosEmisor.setNumRuc(detResumBoletas.getDetResumBoletaPK().getNumRuc());
                    datosEmisor.setDesRazonSocialEmis(obtenerValorNodeRazonSocial(document, detResumBoletas.getDetResumBoletaPK().getCodCpe()));
                    utilLog.imprimirLog(ConstantesUtils.LEVEL_INFO, "datosEmisor.getDesRazonSocialEmis===="+datosEmisor.getDesRazonSocialEmis());
                    
                    datosEmisor.setDesNomComercialEmis(obtenerValorNodeRazonComercial(document,detResumBoletas.getDetResumBoletaPK().getCodCpe()));

                    datosReceptor.setCodDocIdeRecep(detResumBoletas.getCodDocideRecep());
                    comprobanteindivual.setNumSerie(detResumBoletas.getDetResumBoletaPK().getNumSerieCpe());
                    datosReceptor.setNumDocIdeRecep(detResumBoletas.getNumDocideRecep());
                    datosReceptor.setDesRazonSocialRecep("");
                    datosReceptor.setDirDetRecepBoleta("");
                    datosReceptor.setDirDetCliente("");
                    if (!detResumBoletas.getCodDocideRecep().isEmpty() && !detResumBoletas.getCodDocideRecep().equals("-") && !detResumBoletas.getCodDocideRecep().equals("0")) {
                        comprobanteindivual.setDesTipoCpe(parametros.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(detResumBoletas.getCodDocideRecep().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

                    } else {
                    		comprobanteindivual.setDesTipoCpe("-");
                    }
                    
                    if("00".endsWith(detResumBoletas.getCodDocideRecep().trim()) || "0".endsWith(detResumBoletas.getCodDocideRecep().trim())){
                		comprobanteindivual.setDesTipoCpe(parametros.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
                	}
                    comprobanteindivual.setDesMoneda(parametros.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(detResumBoletas.getCodMoneda())).findFirst().map(TiposMoneda::getDesMoneda).orElse("-"));
                    comprobanteindivual.setDesSimbolo(parametros.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(detResumBoletas.getCodMoneda())).findFirst().map(TiposMoneda::getCodSimbolo).orElse("-"));
                    comprobanteindivual.setCodCpe(detResumBoletas.getDetResumBoletaPK().getCodCpe());
                    comprobanteindivual.setNumCpe(detResumBoletas.getNumDesde());
                    comprobanteindivual.setEsResumen(true);
                    comprobanteindivual.setCodMoneda(detResumBoletas.getCodMoneda());
                    comprobanteindivual.setFecEmision(detResumBoletas.getFecEmision());
                    comprobanteindivual.setFecRegistro(detResumBoletas.getFecEmision());
                    comprobanteindivual.setCodTipTransaccion("");
                    comprobanteindivual.setIndEstadoCpe(detResumBoletas.getIndEstado());
                    comprobanteindivual.setIndProcedencia(Constantes.PROCEDENCIA_GEM);
                    comprobanteindivual.setDesMtoTotalLetras(Numero.numerosALetras(detResumBoletas.getMtoImporteTotal()) + " "
                            + ComprobanteUtilBean.getDescripcionMoneda(detResumBoletas.getCodMoneda()));
                    comprobanteindivual.setIndTaxFree("");
                    comprobanteindivual.setIndItinerante("");
                    comprobanteindivual.setDesObservacion("");


                    ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();

                    procedenciaMasiva.setMtoTotalValVentaGrabado(detResumBoletas.getMtoGravadoIgv() != null ? detResumBoletas.getMtoGravadoIgv() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoTotalValVentaInafecto(detResumBoletas.getMtoInafectoIgv() != null ? detResumBoletas.getMtoInafectoIgv() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoTotalValVentaExonerado(detResumBoletas.getMtoExoneradoIgv() != null ? detResumBoletas.getMtoExoneradoIgv() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoTotalValVentaExportacion(detResumBoletas.getMtoExportaciones() != null ? detResumBoletas.getMtoExportaciones() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoSumOtrosTributos(detResumBoletas.getMtoTotalOtros() != null ? detResumBoletas.getMtoTotalOtros() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoSumOtrosCargos(detResumBoletas.getMtoTotalCargos() != null ? detResumBoletas.getMtoTotalCargos() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoSumISC(detResumBoletas.getMtoTotalIsc() != null ? detResumBoletas.getMtoTotalIsc() : BigDecimal.ZERO);
                    
                    
                    procedenciaMasiva.setMtoSumIGV(detResumBoletas.getMtoTotalIgv() != null ? detResumBoletas.getMtoTotalIgv() : BigDecimal.ZERO);
                    
                    if (procedenciaMasiva.getMtoSumIGV().compareTo(BigDecimal.ZERO) == 0) {
                    	RubrosPK rubrosPk = new RubrosPK();
                    	List<Rubros> rubros;
                    	List<Rubros> rubros1;
                    	rubrosPk.setNumSerieCpe(detResumBoletas.getDetResumBoletaPK().getNumSerieCpe());
        				rubrosPk.setCodCpe(detResumBoletas.getDetResumBoletaPK().getCodCpe());
        				rubrosPk.setNumRuc(detResumBoletas.getDetResumBoletaPK().getNumRuc());
        				rubrosPk.setNumCpe(detResumBoletas.getNumDesde());
        				rubrosPk.setNumFilaItem(detResumBoletas.getDetResumBoletaPK().getNumLinea());
                    	rubrosPk.setCodRubro(Constantes.COD_RUBRO_RESUMEN_IVAP);
                    	rubros = rubrosRepository.obtenerRubrosResumenIvap(rubrosPk);
                        procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_432));
                        procedenciaMasiva.setMtoSumIGV(null);
                        
                        rubrosPk.setCodRubro(Constantes.COD_RUBRO_RESUMEN_GRAVADO_IVAP);
                        rubros1 = rubrosRepository.obtenerRubrosResumenIvap(rubrosPk);
                        procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros1, RubrosEnum.RUBRO_440));
                        
                    } 
                    
                    
                    procedenciaMasiva.setMtoSumICBPER(detResumBoletas.getMtoIcbper() != null ? detResumBoletas.getMtoIcbper() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoImporteTotal(detResumBoletas.getMtoImporteTotal() != null ? detResumBoletas.getMtoImporteTotal() : BigDecimal.ZERO);
                    procedenciaMasiva.setMtoTotalValVentaGratuito(detResumBoletas.getMtoOperaGratuita() != null ? detResumBoletas.getMtoOperaGratuita() : BigDecimal.ZERO);
                    DocumentoRelacionadoPK documentoRelacionadoPK = new DocumentoRelacionadoPK();
                    documentoRelacionadoPK.setNumSerieCpe(detResumBoletas.getDetResumBoletaPK().getNumSerieCpe());
                    documentoRelacionadoPK.setCodCpe(detResumBoletas.getDetResumBoletaPK().getCodCpe());
                    documentoRelacionadoPK.setNumRuc(request.getNumRucEmisor());
                    List<InformacionDocumentosRelacionados> informacionDocumentosRelacionados = new ArrayList<>();

                    List<DocumentoRelacionado> documentoRelacionados = Optional
                            .ofNullable(docRelacionadoRepository.obtenerDocumentosRelacionados(documentoRelacionadoPK))
                            .orElse(null);

                    for (DocumentoRelacionado documentoRel : documentoRelacionados) {
                    	String codAgrup = Arrays.asList(LIST_AGRUP_REL_FACBOL_CATALOGO1)
                    			.contains(documentoRel.getCodRelacion()) 
                          ? Constantes.COD_AGRUP_REL_CATALOGO1 : 
                            Arrays.asList(LIST_AGRUP_REL_FACBOL_CATALOGO12)
                            .contains(documentoRel.getCodRelacion())
                            ? Constantes.COD_AGRUP_REL_CATALOGO12:"-";
                        InformacionDocumentosRelacionados infdoc = new InformacionDocumentosRelacionados();
                        if (parametros.getTiposComprobanteRelacionado().stream().anyMatch(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel().equals(documentoRel.getDocumentoRelacionadoPK().getCodCpe()) && tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))) {
                            infdoc.setCodCpeRel(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
                            infdoc.setNumDocRel(documentoRel.getDocumentoRelacionadoPK().getNumDocRel());
                            infdoc.setNumSerieDocRel(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel());
                            infdoc.setDesCpeRel(parametros.getTiposComprobanteRelacionado().stream().filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel().equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())  && tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup)).findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
                            infdoc.setCodAgrupProc(codAgrup);
                            
                        }
                        informacionDocumentosRelacionados.add(infdoc);
                        comprobanteindivual.setInformacionDocumentosRelacionados(informacionDocumentosRelacionados);

                    }
                    if (comprobanteindivual.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO) || comprobanteindivual.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO)) {
                        DatoRelacionado datoRelacionado = new DatoRelacionado();
                        datoRelacionado.setCodTipNota("");
                        datoRelacionado.setDesMotivo("-");
                        List<DocumentosModifica> lstdocumentosModifica = new ArrayList<>();
                        DocumentosModifica documentosModifica = new DocumentosModifica();
                        documentosModifica.setCodCpeRelac(detResumBoletas.getCodCpeModif());
                        documentosModifica.setNumCpeRelac(detResumBoletas.getNumCpeModif());
                        documentosModifica.setNumSerieRelac(detResumBoletas.getNumSeriecpeModif());
                        lstdocumentosModifica.add(documentosModifica);

                        datoRelacionado.setDocumentosModificaList(lstdocumentosModifica);
                        comprobanteindivual.setDatoDodRelacionado(datoRelacionado);
                    }
                    comprobanteindivual.setDatosEmisor(datosEmisor);
                    comprobanteindivual.setDatosReceptor(datosReceptor);
                    comprobanteindivual.setProcedenciaMasiva(procedenciaMasiva);
                    
                    
                    
                    ComprobantePK comprobantePK= new ComprobantePK();
                    
                    comprobantePK.setNumRuc(comprobanteindivual.getDatosEmisor().getNumRuc());
                    comprobantePK.setCodCpe(comprobanteindivual.getCodCpe());
                    comprobantePK.setNumSerieCpe(comprobanteindivual.getNumSerie());
                    comprobantePK.setNumCpe(comprobanteindivual.getNumCpe());
                    
                    Percepcion comprobantePercepcion = Optional
                            .ofNullable(percepcionRepository.obtenerComprobante(comprobantePk,comprobanteindivual.getDatosReceptor().getNumDocIdeRecep()))
                            .orElse(null);
                    if (comprobantePercepcion!=null){
                    
                    List<InformacionPercepcion> informacionPercepcions = new ArrayList<>();
                    
                    InformacionPercepcion informacionPercepcion = new InformacionPercepcion();
                    informacionPercepcion.setMtoPercepcion(comprobantePercepcion.getMtoTotalCobrado());
                    informacionPercepcion.setMtoTotalPercepcion(comprobantePercepcion.getMtoTotalPer());
                    informacionPercepcion.setDesLeyenda(comprobantePercepcion.getDesObservacion().trim());
                    informacionPercepcions.add(informacionPercepcion);
                    
                    comprobanteindivual.setInformacionPercepcions(informacionPercepcions);
                    }
  
                    comprobantes.add(comprobanteindivual);
                }
            }
        }



        return comprobantes;
    }

    @Override
    public List<Comprobantes> obtenerComprobanteMasiva(ComprobanteMasivaRequestDTO request) throws Exception {

        List<Comprobantes> comprobantesList = new ArrayList<>();
        ParametrosSer parametros = parametroRepository.listarParametros();

        ComprobantePK comprobantePk = new ComprobantePK();
        comprobantePk.setCodCpe(request.getCodCpe());
        boolean noEsResumen=true;
        boolean siDocRel=false;
        String codDocRel=Constantes.COD_CPE_NINGUNO;
        if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)||request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)||
        		request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA)){ 
        	noEsResumen=false;
        }
        
        if(request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_BOLETA)||request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)||
           request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_BOLETA)||request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)){
            codDocRel=Constantes.COD_CPE_BOLETA;
        }
        if(request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_FACTURA)||request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_FACTURA)){
            codDocRel=Constantes.COD_CPE_FACTURA;
        }
        
        if (request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_BOLETA) || request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_FACTURA) || request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_NOTA_CREDITO);
        }
        if (request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_FACTURA) || request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_BOLETA) || request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_NOTA_DEBITO);
        }
        if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_BOLETA);
        }
        String numRucEmisor = "";
        if (request.getCodFiltroCpe().contains(Constantes.COD_CPE_EMISOR)) {
            if (Objects.isNull(request.getNumRucEmisor()) || request.getNumRucEmisor().isEmpty()) {
                numRucEmisor = request.getNumRucHeader();
                comprobantePk.setNumRuc(numRucEmisor);
            } else {
                numRucEmisor = request.getNumRucEmisor();
                comprobantePk.setNumRuc(request.getNumRucEmisor());
            }
        } else {
            numRucEmisor = request.getNumRucEmisor() == null ? "" : request.getNumRucEmisor();
            comprobantePk.setNumRuc(request.getNumRucEmisor());
        }

        comprobantePk.setNumSerieCpe(request.getNumSerieCpe());
        if (Objects.isNull(request.getNumDocIde()) && request.getCodFiltroCpe().equals(Constantes.COD_CPE_RECEPTOR)) {
            request.setNumDocIde(request.getNumRucHeader());
        }
        int cantidad=0;
        if (noEsResumen) {
        	cantidad = Optional
                    .ofNullable(comprobanteRepository.obtenerNumeroRegistrosxFiltro(comprobantePk, request.getFecEmisionIni(), request.getFecEmisionFin(), request.getNumCpeIni(), request.getNumCpeFin(), request.getCodDocIde(), request.getNumDocIde(), request.getCodEstado()))
                    .orElse(0);
            if (cantidad > Integer.parseInt(properties.getLimit())) {
            	throw new UnprocessableEntityException(new ErrorMessage("231", "Debe mejorar el filtro b\u00FAsqueda porque excede los "+properties.getLimit()+" comprobantes."));
            }
        	List<Comprobante> comprobantes = Optional
                    .ofNullable(comprobanteRepository.obtenerComprobantesxFiltro(comprobantePk, request.getFecEmisionIni(), request.getFecEmisionFin(), request.getNumCpeIni(), request.getNumCpeFin(), request.getCodDocIde(), request.getNumDocIde(), request.getCodEstado()))
                    .orElse(null);
            List<Comprobante> comprobantesnuevo = comprobantes.stream()
                    .limit(properties.getCntComp())
                    .collect(Collectors.toList());
            for (Comprobante comprobante : comprobantesnuevo) {
                Comprobantes comprobanteindivual = null;
                
                DocumentoRelacionadoPK documentoRelacionadoPK = new DocumentoRelacionadoPK();
                documentoRelacionadoPK.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
                documentoRelacionadoPK.setCodCpe(comprobante.getComprobantePK().getCodCpe());
                documentoRelacionadoPK.setNumRuc(comprobante.getComprobantePK().getNumRuc());
                documentoRelacionadoPK.setNumCpe(comprobante.getComprobantePK().getNumCpe());
                List<DocumentoRelacionado> documentoRelacionados = Optional
                        .ofNullable(docRelacionadoRepository.obtenerDocumentosRelacionados(documentoRelacionadoPK))
                        .orElse(null);
                siDocRel=false;
                if(request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_FACTURA)||request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_FACTURA) ||
                       request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_BOLETA)||request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_BOLETA)){
                        for (DocumentoRelacionado documentoRelacionado : documentoRelacionados) {
                            if(documentoRelacionado.getDocumentoRelacionadoPK().getCodDocRel().equals(codDocRel)){
                                siDocRel=true;
                                break;
                            }
                        }
                    }
                    if((codDocRel.equals(Constantes.COD_CPE_FACTURA)||codDocRel.equals(Constantes.COD_CPE_BOLETA))&&siDocRel){
    
                        if (comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO) || comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO)) {
                            comprobanteindivual = llenarDatosNotasMasivo(comprobante, parametros, comprobante.getFecEmision());
                            comprobanteindivual.setCodDocRel(codDocRel);
                        }
                        

                    }else if(codDocRel.equals(Constantes.COD_CPE_NINGUNO)){
                        if (comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_FACTURA) || comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_BOLETA)) {
                            comprobanteindivual = llenarDatosFacturaBoletaMasivo(comprobante, parametros, comprobante.getFecEmision());
                            comprobanteindivual.setCodDocRel(codDocRel);
                        }
                        if (comprobante.getComprobantePK().getCodCpe().equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)) {
                            comprobanteindivual = llenarDatosLiquidacionMasivo(comprobante, parametros, comprobante.getFecEmision());
                            comprobanteindivual.setCodDocRel(codDocRel);
                        }
                        
                    }
                    
                if(comprobanteindivual!=null) {
                comprobantesList.add(comprobanteindivual);
                }
            }
        }
        
        if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA) 
        		|| request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO) 
        		|| request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)) {
                DetResumBoletaPK detResumBoletaPK = new DetResumBoletaPK();
                detResumBoletaPK.setCodCpe(comprobantePk.getCodCpe());
            detResumBoletaPK.setNumRuc(comprobantePk.getNumRuc());
                detResumBoletaPK.setNumSerieCpe(request.getNumSerieCpe());

                int cantidadDetresum = Optional
                        .ofNullable(detResumBoletaRepository.obtenerRegistrosResumBoletaxFiltro(detResumBoletaPK, request.getFecEmisionIni(), request.getFecEmisionFin(), request.getNumCpeIni(), request.getNumCpeFin(), request.getCodDocIde(), request.getNumDocIde(), request.getCodEstado()))
                        .orElse(0);
                if (cantidadDetresum >= Integer.parseInt(properties.getLimit())) {
                	throw new UnprocessableEntityException(new ErrorMessage("231", "Debe mejorar el filtro b\u00FAsqueda porque excede los "+properties.getLimit()+" comprobantes."));
                }

                List<DetResumBoleta> detResumBoletas = Optional
                        .ofNullable(detResumBoletaRepository.obtenerResumBoletaxFiltro(detResumBoletaPK, request.getFecEmisionIni(), request.getFecEmisionFin(), request.getNumCpeIni(), request.getNumCpeFin(), request.getCodDocIde(), request.getNumDocIde(), request.getCodEstado()))
                        .orElse(null);
                if (Objects.nonNull(detResumBoletas)) {
                    List<DetResumBoleta> detreusmenuevo = detResumBoletas.stream()
                            .limit(properties.getCntComp())
                            .collect(Collectors.toList());
                    
                    for (DetResumBoleta dtresum : detreusmenuevo) {
                        
                        DocumentoRelacionadoPK documentoRelacionadoPK = new DocumentoRelacionadoPK();
                        documentoRelacionadoPK.setNumSerieCpe(dtresum.getDetResumBoletaPK().getNumSerieCpe());
                        documentoRelacionadoPK.setCodCpe(dtresum.getDetResumBoletaPK().getCodCpe());
                        documentoRelacionadoPK.setNumRuc(dtresum.getDetResumBoletaPK().getNumRuc());
                        
                        DatosEmisor datosEmisor = new DatosEmisor();
                        DatosReceptor datosReceptor = new DatosReceptor();
                        Comprobantes comprobanteindivual = new Comprobantes();

                        datosEmisor.setNumRuc(dtresum.getDetResumBoletaPK().getNumRuc());
                        datosEmisor.setDesRazonSocialEmis(obtenerRazonSocial(dtresum.getDetResumBoletaPK().getNumRuc()));
                        datosReceptor.setCodDocIdeRecep(dtresum.getCodDocideRecep());
                        comprobanteindivual.setNumSerie(dtresum.getDetResumBoletaPK().getNumSerieCpe());
                        datosReceptor.setNumDocIdeRecep(dtresum.getNumDocideRecep());
                        if(dtresum.getNumDocideRecep().trim().equals("6")||dtresum.getNumDocideRecep().trim().equals("06")){
                        datosReceptor.setDesRazonSocialRecep(obtenerRazonSocial(dtresum.getNumDocideRecep().trim()));
                        }else{
                        datosReceptor.setDesRazonSocialRecep("-");
                        }

                        comprobanteindivual.setDatosEmisor(datosEmisor);
                        comprobanteindivual.setDatosReceptor(datosReceptor);
                        comprobanteindivual.setCodCpe(dtresum.getDetResumBoletaPK().getCodCpe());
                        comprobanteindivual.setNumCpe(dtresum.getNumDesde());
                        comprobanteindivual.setEsResumen(true);
                        comprobanteindivual.setCodCpe(dtresum.getDetResumBoletaPK().getCodCpe());
                        comprobanteindivual.setNumCpe(dtresum.getNumDesde());
                        comprobanteindivual.setFecEmision(dtresum.getFecEmision());
                        comprobanteindivual.setIndEstadoCpe(dtresum.getIndEstado());
                        comprobanteindivual.setIndProcedencia(Constantes.PROCEDENCIA_RESUMEN);
                        comprobanteindivual.setCodDocRel(codDocRel);
                        
                        ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();

                        procedenciaMasiva.setMtoTotalValVentaGrabado(dtresum.getMtoGravadoIgv() != null ? dtresum.getMtoGravadoIgv() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoTotalValVentaInafecto(dtresum.getMtoInafectoIgv() != null ? dtresum.getMtoInafectoIgv() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoTotalValVentaExonerado(dtresum.getMtoExoneradoIgv() != null ? dtresum.getMtoExoneradoIgv() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoTotalValVentaExportacion(dtresum.getMtoExportaciones() != null ? dtresum.getMtoExportaciones() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoSumOtrosTributos(dtresum.getMtoTotalOtros() != null ? dtresum.getMtoTotalOtros() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoSumOtrosCargos(dtresum.getMtoTotalCargos() != null ? dtresum.getMtoTotalCargos() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoSumISC(dtresum.getMtoTotalIsc() != null ? dtresum.getMtoTotalIsc() : BigDecimal.ZERO);
                        

                        procedenciaMasiva.setMtoSumIGV(dtresum.getMtoTotalIgv() != null ? dtresum.getMtoTotalIgv() : BigDecimal.ZERO);
                        
                        if (procedenciaMasiva.getMtoSumIGV().compareTo(BigDecimal.ZERO) == 0) {
                        	RubrosPK rubrosPk = new RubrosPK();
                        	List<Rubros> rubros;
                        	List<Rubros> rubros1;
                        	rubrosPk.setNumSerieCpe(dtresum.getDetResumBoletaPK().getNumSerieCpe());
            				rubrosPk.setCodCpe(dtresum.getDetResumBoletaPK().getCodCpe());
            				rubrosPk.setNumRuc(dtresum.getDetResumBoletaPK().getNumRuc());
            				rubrosPk.setNumCpe(dtresum.getNumDesde());
            				rubrosPk.setNumFilaItem(dtresum.getDetResumBoletaPK().getNumLinea());
                        	rubrosPk.setCodRubro(Constantes.COD_RUBRO_RESUMEN_IVAP);
                        	rubros = rubrosRepository.obtenerRubrosResumenIvap(rubrosPk);
                            procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_432));
                            procedenciaMasiva.setMtoSumIGV(null);
                            
                            rubrosPk.setCodRubro(Constantes.COD_RUBRO_RESUMEN_GRAVADO_IVAP);
                            rubros1 = rubrosRepository.obtenerRubrosResumenIvap(rubrosPk);
                            procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros1, RubrosEnum.RUBRO_440));
                            
                        } 
                        
                        
                        procedenciaMasiva.setMtoSumICBPER(dtresum.getMtoIcbper() != null ? dtresum.getMtoIcbper() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoImporteTotal(dtresum.getMtoImporteTotal() != null ? dtresum.getMtoImporteTotal() : BigDecimal.ZERO);
                        procedenciaMasiva.setMtoTotalValVentaGratuito(dtresum.getMtoOperaGratuita() != null ? dtresum.getMtoOperaGratuita() : BigDecimal.ZERO);

                        comprobanteindivual.setProcedenciaMasiva(procedenciaMasiva);
                        comprobantesList.add(comprobanteindivual);
                    }
                }
            }


        if (request.getCodCpe().equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)) {
       	
            List<DetalleLiqPei> detalleLiqPeis = Optional
                    .ofNullable(detPeiRepository.obtenerLiquidacionPeixFiltro(request.getNumRucEmisor(), request.getCodCpe(), request.getNumSerieCpe(), request.getCodEstado(), request.getCodDocIde(), request.getNumDocIde(), request.getNumCpeIni(), request.getNumCpeFin(),request.getFecEmisionIni(),request.getFecEmisionFin()))
                    .orElse(null);
            if (Objects.nonNull(detalleLiqPeis)) {
            	cantidad = cantidad+detalleLiqPeis.size();
                if (cantidad > Integer.parseInt(properties.getLimit())) {
                    throw new UnprocessableEntityException(new ErrorMessage("231", "Debe mejorar el filtro b\u00FAsqueda porque excede los "+properties.getLimit()+" comprobantes o el tamaÃƒÂ±o supera lo permitido."));
                }
                List<DetalleLiqPei> detalleLiqPeisNuevo = detalleLiqPeis.stream()
                        .limit(properties.getCntComp())
                        .collect(Collectors.toList());
                for (DetalleLiqPei detalleLiqPei : detalleLiqPeisNuevo) {
                    DatosEmisor datosEmisor = new DatosEmisor();
                    DatosReceptor datosReceptor = new DatosReceptor();
                    Comprobantes comprobanteindivual = new Comprobantes();
                    datosEmisor.setNumRuc(detalleLiqPei.getNumDocIdComp());
                    datosEmisor.setDesRazonSocialEmis(detalleLiqPei.getDesNombreComp());
                    datosReceptor.setCodDocIdeRecep(detalleLiqPei.getCodDocIdVend());
                    comprobanteindivual.setNumSerie(detalleLiqPei.getNumSerie());
                    datosReceptor.setNumDocIdeRecep(detalleLiqPei.getNumDocIdVend());
                    datosReceptor.setDesRazonSocialRecep(detalleLiqPei.getDesNombreVend());
                    datosReceptor.setDirDetCliente(detalleLiqPei.getDesDirVend());
                    datosReceptor.setDirDetVendedor(detalleLiqPei.getDesDirVend() + " " + detalleLiqPei.getDesDeptDir() + "-" + detalleLiqPei.getDesProvDir() + "-" + detalleLiqPei.getDesDistDir());
                    if (!detalleLiqPei.getCodDocIdVend().isEmpty() && !detalleLiqPei.getCodDocIdVend().equals("-")) {
                        comprobanteindivual.setDesTipoCpe(parametros.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(detalleLiqPei.getCodDocIdVend().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

                    } else {
                    		comprobanteindivual.setDesTipoCpe("-");
                    }
                    if("00".endsWith(detalleLiqPei.getCodDocIdVend().trim()) || "0".endsWith(detalleLiqPei.getCodDocIdVend().trim())){
                		comprobanteindivual.setDesTipoCpe(parametros.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
                	}
                    comprobanteindivual.setCodCpe(detalleLiqPei.getCodCdp());
                    comprobanteindivual.setNumCpe(Integer.parseInt(detalleLiqPei.getNumCdp()));
                    comprobanteindivual.setCodMoneda(detalleLiqPei.getCodMoneda());
                    comprobanteindivual.setFecEmision(ComprobanteUtilBean.formatFecha(detalleLiqPei.getFecEmisCdp()));

                    comprobanteindivual.setIndProcedencia(Constantes.PROCEDENCIA_PEI);
                    if(detalleLiqPei.getCodTipoEnvio().equals(Constantes.COD_ESTADO_PEI_ACTIVO)){
                    comprobanteindivual.setIndEstadoCpe(Constantes.COD_ESTADO_ACTIVO);
                    }
                    if(detalleLiqPei.getCodTipoEnvio().equals(Constantes.COD_ESTADO_PEI_ANULADO)){
                    comprobanteindivual.setIndEstadoCpe(Constantes.COD_ESTADO_ANULADO);
                    }
                    
                    comprobanteindivual.setDesObservacion("");
               
                    comprobanteindivual.setDatosEmisor(datosEmisor);
                    comprobanteindivual.setDatosReceptor(datosReceptor);
                    comprobanteindivual.setCodDocRel(codDocRel);
                    comprobantesList.add(comprobanteindivual);
                }
            }
        }

        return comprobantesList;
    }


    @Override
    public Date obtenerFechaEmision(ComprobanteIndividualRequestDTO request) throws ParseException {
        ComprobantePK comprobantePk = new ComprobantePK();
        comprobantePk.setCodCpe(request.getCodCpe());
        comprobantePk.setNumCpe(request.getNumCpe().intValue());
        comprobantePk.setNumRuc(request.getNumRucEmisor());
        comprobantePk.setNumSerieCpe(request.getNumSerieCpe());
        if (request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_BOLETA) || request.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO_FACTURA) || request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_NOTA_CREDITO);
        }
        if (request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_FACTURA) || request.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO_BOLETA) || request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_NOTA_DEBITO);
        }
        if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA)) {
            comprobantePk.setCodCpe(Constantes.COD_CPE_BOLETA);
        }
        Optional<Date> fecha;
        if (request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA) || request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO) || request.getCodCpe().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)) {

            fecha = Optional.ofNullable(detResumBoletaRepository.obtenerFechaEmision(comprobantePk.getNumRuc(), comprobantePk.getCodCpe(), comprobantePk.getNumSerieCpe(), comprobantePk.getNumCpe(), comprobantePk.getNumCpe()));

        } else {
            fecha = Optional.ofNullable(comprobanteRepository.obtenerFechaEmision(comprobantePk));

            if (!fecha.isPresent()) {
                fecha = Optional.ofNullable(detPeiRepository.obtenerFechaEmisionPei(comprobantePk));
            }


        }
        if (fecha.isPresent()) {
            return fecha.get();

        } else {
            return null;
        }
    }
    
    @Override
    public File recuperaComprobanteXml(String numRuc, String numCpe, String codCpe, String numSerieCpe) {
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                "XmlComprobanteServiceImpl.recuperaComprobanteXmlService - INICIO");
        
        try {
            ComprobanteXML comprobanteXml = null;
            ComprobanteFestore comprobanteXMLFestore = new ComprobanteFestore();
            ComprobantePK comprobantePK = new ComprobantePK();
            comprobantePK.setNumCpe(Integer.valueOf(numCpe));
            comprobantePK.setNumSerieCpe(numSerieCpe);
            comprobantePK.setCodCpe(codCpe);
            comprobantePK.setNumRuc(numRuc);
            Optional<String> procedencia = Optional.ofNullable(comprobanteRepository.obtenerProcedencia(comprobantePK));
            if (procedencia.isPresent()) {
                String fileName = "";
                
                if (procedencia.get().equals(Constantes.PROCEDENCIA_PORTAL) 
                		|| procedencia.get().equals(Constantes.PROCEDENCIA_APP_EMPRENDER) 
                		|| procedencia.get().equals(Constantes.PROCEDENCIA_APP_SUNAT)) {
                	
                	
                	
                    comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                    fileName = comprobanteXml.getDesNombre();
                
                } else if (procedencia.get().equals(Constantes.PROCEDENCIA_GEM) 
                		|| procedencia.get().equals(Constantes.PROCEDENCIA_OSE)) {

                    comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                    fileName = numRuc + "-" + codCpe
                            + "-" + numSerieCpe + "-"
                            + numCpe + ".zip";
                } else if (procedencia.get().equals(Constantes.PROCEDENCIA_CONTINGENCIA_GEM_PORTAL)) {

                    comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                    if (Objects.nonNull(comprobanteXml)) {
                        fileName = comprobanteXml.getDesNombre();
                    } else {
                        comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                        fileName = numRuc + "-" + codCpe
                                + "-" + numSerieCpe + "-"
                                + numCpe + ".zip";
                    }
                }
								File newFileXml = new File(fileName);
								if (Objects.nonNull(comprobanteXml)) {
									FileUtils.writeStringToFile(newFileXml, comprobanteXml.getArcXml());

								} else if (Objects.nonNull(comprobanteXMLFestore)) {
									byte[] byteArray = comprobanteXMLFestore.arcInput;
									FileUtils.writeByteArrayToFile(newFileXml, byteArray);
									
								} else {
									utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                      "XmlComprobanteServiceImpl.recuperaComprobanteXmlService - FIN - No se encontro comprobante on-premise ");
									return null;
								}
                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                        "XmlComprobanteServiceImpl.recuperaComprobanteXmlService - FIN");
                return newFileXml;

            } else {
            	
            	utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                        "=================================>No tiene Procedencia: ");
                String fileName = "";
                comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlResumenService(numRuc, numCpe, codCpe, numSerieCpe);
                fileName = numRuc + "-" + codCpe
                        + "-" + numSerieCpe + "-"
                        + numCpe + ".zip";
                if (Objects.nonNull(comprobanteXMLFestore)) {
	                File newFileXml = new File(fileName);
	                byte[] byteArray = comprobanteXMLFestore.arcInput;
	                FileUtils.writeByteArrayToFile(newFileXml, byteArray);
	                return newFileXml;
                }else{
                	return null;
                }

            }


        } catch (Exception e) {
            return null;
        }

    }
    
    
    @Override
    public Document recuperaComprobanteXmlDoc(String numRuc, String numCpe, String codCpe, String numSerieCpe) {
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                "XmlComprobanteServiceImpl.recuperaComprobanteXmlService - INICIO");
        Document doc=null;
        try {
            ComprobanteXML comprobanteXml = null;
            ComprobanteFestore comprobanteXMLFestore = new ComprobanteFestore();
            ComprobantePK comprobantePK = new ComprobantePK();
            comprobantePK.setNumCpe(Integer.valueOf(numCpe));
            comprobantePK.setNumSerieCpe(numSerieCpe);
            comprobantePK.setCodCpe(codCpe);
            comprobantePK.setNumRuc(numRuc);
            Optional<String> procedencia = Optional.ofNullable(comprobanteRepository.obtenerProcedencia(comprobantePK));
            if (procedencia.isPresent()) {
                String fileName = "";

                if (procedencia.get().equals(Constantes.PROCEDENCIA_PORTAL) 
                		|| procedencia.get().equals(Constantes.PROCEDENCIA_APP_EMPRENDER) 
                		|| procedencia.get().equals(Constantes.PROCEDENCIA_APP_SUNAT)) {

                    comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                    fileName = comprobanteXml.getDesNombre();
                
                } else if (procedencia.get().equals(Constantes.PROCEDENCIA_GEM) 
                		|| procedencia.get().equals(Constantes.PROCEDENCIA_OSE)) {

                    comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                    fileName = numRuc + "-" + codCpe
                            + "-" + numSerieCpe + "-"
                            + numCpe + ".zip";
                } else if (procedencia.get().equals(Constantes.PROCEDENCIA_CONTINGENCIA_GEM_PORTAL)) {

                    comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                    if (Objects.nonNull(comprobanteXml)) {
                        fileName = comprobanteXml.getDesNombre();
                    } else {
                        comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
                        fileName = numRuc + "-" + codCpe
                                + "-" + numSerieCpe + "-"
                                + numCpe + ".zip";
                    }
                }
		if (Objects.nonNull(comprobanteXml)) {
                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"comprobanteXml.getArcXml()======"+comprobanteXml.getArcXml());
                String archivoString=comprobanteXml.getArcXml().replace("ï¿½", ""); 
                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"archivoString======"+archivoString);
                    try {
                    DocumentBuilderFactory factory= DocumentBuilderFactory.newInstance();
                    DocumentBuilder builder=factory.newDocumentBuilder();
                    doc=builder.parse(new InputSource(new StringReader(archivoString)));
                    } catch (Exception e) {
                        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"Error="+e.getMessage());
        	            File newFile = new File(fileName);
        	            if (Objects.nonNull(comprobanteXMLFestore.arcInput)) {
        	              FileUtils.writeByteArrayToFile(newFile, comprobanteXMLFestore.arcInput);
        	            }
        	            if (fileName.endsWith(".zip")) {
        	                newFile = unzipFile(fileName);
        	            }
        	            return fileToDocument(newFile);
                    
                    }
                    
                } else if (Objects.nonNull(comprobanteXMLFestore)) {
		byte[] byteArray = comprobanteXMLFestore.arcInput;
                String archivoString=new String(byteArray,"ISO-8859-1");
                    try {
                    DocumentBuilderFactory factory= DocumentBuilderFactory.newInstance();
                    DocumentBuilder builder=factory.newDocumentBuilder();
                    doc=builder.parse(new InputSource(new StringReader(archivoString)));
                    } catch (Exception e) {
                        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"Error="+e.getMessage());
        	            File newFile = new File(fileName);
        	            if (Objects.nonNull(comprobanteXMLFestore.arcInput)) {
        	              FileUtils.writeByteArrayToFile(newFile, comprobanteXMLFestore.arcInput);
        	            }
        	            if (fileName.endsWith(".zip")) {
        	                newFile = unzipFile(fileName);
        	            }
        	            return fileToDocument(newFile);
                    }
                } else {
		return null;
                }
                return doc;
            } else {
                String fileName = "";
                comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlResumenService(numRuc, numCpe, codCpe, numSerieCpe);
                fileName = numRuc + "-" + codCpe
                        + "-" + numSerieCpe + "-"
                        + numCpe + ".zip";
                if (Objects.nonNull(comprobanteXMLFestore)) {

	                byte[] byteArray = comprobanteXMLFestore.arcInput;
                        String archivoString=new String(byteArray,"ISO-8859-1");
                        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"archivoString======"+archivoString);
                        try {
                        DocumentBuilderFactory factory= DocumentBuilderFactory.newInstance();
                        DocumentBuilder builder=factory.newDocumentBuilder();
                        doc=builder.parse(new InputSource(new StringReader(archivoString)));

                        } catch (Exception e) {
                            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"Error="+e.getMessage());
            	            File newFile = new File(fileName);
            	            if (Objects.nonNull(comprobanteXMLFestore.arcInput)) {
            	              FileUtils.writeByteArrayToFile(newFile, comprobanteXMLFestore.arcInput);
            	            }
            	            if (fileName.endsWith(".zip")) {
            	                newFile = unzipFile(fileName);
            	            }
            	            return fileToDocument(newFile);
                        }
	                return doc;
                }else{
                	return null;
                }

            }


        } catch (Exception e) {
            return null;
        }

    }
    
    
    private Document recuperaComprobanteXmlDocNube(ArchivoRequestDTO archivoRequestDTO) {
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                "XmlComprobanteServiceImpl.recuperaComprobanteXmlService - INICIO");
        Document doc=null;
        try {
                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"===Obteniendo Archivo XML Nube======");
        	            File newFile = recuperaComprobanteXmlNube(archivoRequestDTO);
        	            String fileName = newFile.getName();
        	            if (fileName.endsWith(".zip")) {
        	                newFile = unzipFile(fileName);
        	            }
        	            return fileToDocument(newFile);
                    
        } catch (Exception e) {
            return null;
        }

    }
    
    
    
    private Document fileToDocument(File file) {
	    try {
	        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder builder = factory.newDocumentBuilder();
	        Document doc = builder.parse(file);
	        return doc;
	    } catch (Exception e) {
	        e.printStackTrace();
	        return null;
	    }
	}
    
    private File unzipFile(String zipFilePath) {
	    byte[] buffer = new byte[1024];
	    try {
	        ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath));
	        ZipEntry zipEntry = zis.getNextEntry();
	        
	        
	        while (zipEntry != null) {
	        	String entryName = zipEntry.getName();
	        	if (!entryName.startsWith("R") && !zipEntry.isDirectory()) {
	            File newFile = new File(zipEntry.getName());
	            FileOutputStream fos = new FileOutputStream(newFile);
	            int len;
		            while ((len = zis.read(buffer)) > 0) {
		                fos.write(buffer, 0, len);
		            }
		        fos.close();
		        return newFile;
	        	}
	        	zipEntry = zis.getNextEntry();
	        }
	        zis.closeEntry();
	        zis.close();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    return null;
	}

    
    @Override
    public File recuperaComprobanteXmlDesc(String numRuc, String numCpe, String codCpe, String numSerieCpe,String codTipFiltro,String numRucHeader) {
    	utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
    			"XmlComprobanteServiceImpl.recuperaComprobanteXmlService - INICIO");
    	try {
    		ComprobanteXML comprobanteXml = null;
    		ComprobanteFestore comprobanteXMLFestore = new ComprobanteFestore();
    		ComprobantePK comprobantePK = new ComprobantePK();
    		comprobantePK.setNumCpe(Integer.valueOf(numCpe));
    		comprobantePK.setNumSerieCpe(numSerieCpe);
    		comprobantePK.setCodCpe(codCpe);
    		comprobantePK.setNumRuc(numRuc);

    		/* 
    		 * ************************
    		 * Cambio NFS
    		 * ************************
    		 */

    		Calendar calendario = Calendar.getInstance();
    		calendario.setTime(comprobanteRepository.obtenerFechaEmision(comprobantePK));
    		String annio = String.valueOf(calendario.get(Calendar.YEAR));
    		String mes = String.format("%02d", calendario.get(Calendar.MONTH) + 1);
    		String dia = String.format("%02d", calendario.get(Calendar.DAY_OF_MONTH));
    		char ultimoDigitoRuc = numRuc.charAt(numRuc.length() - 1);
    		String cuatroPenultimosRuc=numRuc.substring(6, 10);
    		String ruta=null;
    		ruta= annio +"/"+mes+"/"+dia+"/"+ultimoDigitoRuc+"/"+cuatroPenultimosRuc+"/"+numRuc+"/"+codCpe+"/0";
    		utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
        			"XmlComprobanteServiceImpl.recuperaComprobanteXmlService - Ruta: "+ ruta);
    		String nomArchivo=numRuc + "-"+ codCpe + "-" + numSerieCpe + "-" + numCpe + ".zip";
    		String textoJson = "{\"nomArchivo\":\"" + nomArchivo + "\",\"ruta\":\""+ruta+"\"}";

    		byte[] resultadoNfs = buscarArchivoNFS(textoJson);
    
    		if (resultadoNfs==null) {
    			
    			Optional<String> procedencia = Optional.ofNullable(comprobanteRepository.obtenerProcedencia(comprobantePK));
    			if (procedencia.isPresent()) {
    				String fileName = "";
    				
    				if (procedencia.get().equals(Constantes.PROCEDENCIA_PORTAL) 
    						|| procedencia.get().equals(Constantes.PROCEDENCIA_APP_EMPRENDER) 
    						|| procedencia.get().equals(Constantes.PROCEDENCIA_APP_SUNAT)) {

    					Comprobante comprobante = Optional
    							.ofNullable(comprobanteRepository.obtenerComprobante(comprobantePK, codTipFiltro, numRucHeader))
    							.orElse(null);
    					if (Objects.nonNull(comprobante)) {
    						comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
    						fileName = comprobanteXml.getDesNombre();
    					}

    				} else if (procedencia.get().equals(Constantes.PROCEDENCIA_GEM) 
    						|| procedencia.get().equals(Constantes.PROCEDENCIA_OSE)) {

    					Comprobante comprobante = Optional
    							.ofNullable(comprobanteRepository.obtenerComprobante(comprobantePK, codTipFiltro, numRucHeader))
    							.orElse(null);
    					if (Objects.nonNull(comprobante)) {
    						comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
    						fileName = numRuc + "-" + codCpe
    								+ "-" + numSerieCpe + "-"
    								+ numCpe + ".zip";
    					}
    				} else if (procedencia.get().equals(Constantes.PROCEDENCIA_CONTINGENCIA_GEM_PORTAL)) {
    					Comprobante comprobante = Optional
    							.ofNullable(comprobanteRepository.obtenerComprobante(comprobantePK, codTipFiltro, numRucHeader))
    							.orElse(null);
    					if (Objects.nonNull(comprobante)) {
    						comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
    						if (Objects.nonNull(comprobanteXml)) {
    							fileName = comprobanteXml.getDesNombre();
    						} else {
    							comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlService(numRuc, numCpe, codCpe, numSerieCpe);
    							fileName = numRuc + "-" + codCpe
    									+ "-" + numSerieCpe + "-"
    									+ numCpe + ".zip";
    						}
    					}
    				}
    				File newFileXml = new File(fileName);
    				if (Objects.nonNull(comprobanteXml)) {
    					FileUtils.writeStringToFile(newFileXml, comprobanteXml.getArcXml());

    				} else if (Objects.nonNull(comprobanteXMLFestore)) {
    					byte[] byteArray = comprobanteXMLFestore.arcInput;
    					FileUtils.writeByteArrayToFile(newFileXml, byteArray);

    				} else {
    					utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
    							"XmlComprobanteServiceImpl.recuperaComprobanteXmlService - FIN - No se encontro comprobante on-premise ");
    					return null;
    				}
    				utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
        					"XmlComprobanteServiceImpl.recuperaComprobanteXmlService retorna archivo de la Base de datos");
    				return newFileXml;

    			} else {
    				String fileName = "";
    				comprobanteXMLFestore = compFestoreRepository.findComprobanteXmlResumenService(numRuc, numCpe, codCpe, numSerieCpe);
    				fileName = numRuc + "-" + codCpe
    						+ "-" + numSerieCpe + "-"
    						+ numCpe + ".zip";
    				if (Objects.nonNull(comprobanteXMLFestore)) {
    					File newFileXml = new File(fileName);
    					byte[] byteArray = comprobanteXMLFestore.arcInput;
    					
    					FileUtils.writeByteArrayToFile(newFileXml, byteArray);
    					return newFileXml;
    				}else{
    					return null;
    				}
    			}
    		}else
    		{	
    			String fileName = numRuc + "-" + codCpe + "-" + numSerieCpe + "-" + numCpe + ".zip";
    			String fileNameXml = numRuc + "-" + codCpe + "-" + numSerieCpe + "-" + numCpe + ".xml";
    			File newFileXml = new File(fileName);
    			byte[] byteArray = resultadoNfs;
    			byteArray=compress(byteArray, fileNameXml);
    			FileUtils.writeByteArrayToFile(newFileXml, byteArray);
    			utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
    					"XmlComprobanteServiceImpl.recuperaComprobanteXmlService retorna archivo del NFS");
    			utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
    					"resultado del newFileXml:"+ newFileXml.toString() );
    			return newFileXml;
    		}

    	} catch (Exception e) {
    		return null;
    	}
    }

public byte[] buscarArchivoNFS(String textoJson) {

        String jsonInputString = textoJson;
        HttpURLConnection con = null;
        BufferedReader br = null;

        try {
            // Obtener la URL del servicio y verificar si está habilitado
        	ParametriaRepositoryImpl parametriaRepository = new ParametriaRepositoryImpl();
    		T01paramPK t01paramPK = new T01paramPK();
            t01paramPK.setNumero("967");
            t01paramPK.setTipo("D");
            t01paramPK.setArgumento("INTRES16152601");
            T01param resultado = parametriaRepository.obtenerParametroByPk(t01paramPK);
            String servicioConsulta=null;
            if (resultado != null) {
            	servicioConsulta=resultado.getFuncion().substring(0, 119).trim();
            	String servicioConsultaEstado=resultado.getFuncion().substring(124, 125).trim();
        		if("0".equals(servicioConsultaEstado)) {				
        			throw new RuntimeException("Servicio Web se encuentra inhabilitado.WS:"+"INTRES16152601");}
            } else {
            	utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                        "======================> No se encontraron resultados en el Param");
            	return null;
            }

            URL urlObj = new URL(servicioConsulta);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);
            byte[] decodedBytes = null;
            // Envío del JSON
            try (OutputStream os = con.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            
            int status = con.getResponseCode();
            if (status == HttpURLConnection.HTTP_OK) {
                br = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = br.readLine()) != null) {
                    content.append(inputLine);
                }
                ObjectMapper mapper = new ObjectMapper();
                ArchivoNfsResponseDTO response = mapper.readValue(content.toString(), ArchivoNfsResponseDTO.class);

                if (response.getResults().length > 0) {
                    ArchivoNfsResponseDTO.Result result = response.getResults()[0];
                    utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "NomArchivo::::::>" + result.getNomArchivo());
                    utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "ValArchivo::::::> " + result.getBase64());

                    String base64String = result.getBase64();
                    decodedBytes = Base64.getDecoder().decode(base64String);
                    decodedBytes = decompress(decodedBytes);
                } else {
                    utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "No hay resultados disponibles.");
                }

                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "ResultObject: ");
                return decodedBytes;
            } else {
                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "Error: " + status);
                return null;
            }
        } catch (Exception e) {
            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "consultarNFS - Except: " + e);
            return null;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception e) {
                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "consultarNFS - Except br: " + e);
            }
            if (con != null) {
                con.disconnect();
            }
        }
    }
	
    

    public byte[] decompress(byte[] data) {
    	try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
    		 ZipInputStream zipInputStream = new ZipInputStream(byteArrayInputStream);
    		 ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(data.length)) {

    		byte[] buffer = new byte[2048];
    		ZipEntry entry;

    		while ((entry = zipInputStream.getNextEntry()) != null) {
    			if (entry.getName().contains("R-")) {
    				continue;
    			}
    			int len;
    			while ((len = zipInputStream.read(buffer)) > 0) {
    				byteArrayOutputStream.write(buffer, 0, len);
    			}
    		}
    		return byteArrayOutputStream.toByteArray();

    	} catch (IOException e) {
    		e.printStackTrace(); 
    		return data; 
    	}
    }

    

    public static byte[] compress(byte[] data, String fileName) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream)) {

            ZipEntry zipEntry = new ZipEntry(fileName);
            zipOutputStream.putNextEntry(zipEntry);
            zipOutputStream.write(data);
            zipOutputStream.closeEntry();

            zipOutputStream.finish();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return data;
        }
    }
    
    
    @Override
    public File recuperaComprobanteCdr(String numRuc, String numCpe, String codCpe, String numSerieCpe) {
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                "XmlComprobanteServiceImpl.recuperaComprobanteCdrService - INICIO");
        try {
            ComprobanteFestore comprobanteCDR;
            comprobanteCDR = compFestoreRepository.findComprobanteCDRService(numRuc, numCpe, codCpe, numSerieCpe);
            if (Objects.isNull(comprobanteCDR)) {
                comprobanteCDR = compFestoreRepository.findComprobanteCDRResumenService(numRuc, numCpe, codCpe, numSerieCpe);
            }
            String fileName = numRuc + "-" + codCpe
                    + "-" + numSerieCpe + "-"
                    + numCpe + ".zip";

            byte[] byteArray = comprobanteCDR.arcInput;

            File newFileXml = new File(fileName);
            FileUtils.writeByteArrayToFile(newFileXml, byteArray);
            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                    "XmlComprobanteServiceImpl.recuperaComprobanteCdrService - FIN");
            return newFileXml;
        } catch (Exception e) {
            return null;
        }
    }
    
    @Override
    public File recuperaComprobanteCdrDesc(String numRuc, String numCpe, String codCpe, String numSerieCpe, String codTipFiltro,String numRucHeader) {
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                "XmlComprobanteServiceImpl.recuperaComprobanteCdrService - INICIO");
        try {
            ComprobantePK comprobantePK = new ComprobantePK();
            comprobantePK.setNumCpe(Integer.valueOf(numCpe));
            comprobantePK.setNumSerieCpe(numSerieCpe);
            comprobantePK.setCodCpe(codCpe);
            comprobantePK.setNumRuc(numRuc);
            
            ComprobanteFestore comprobanteCDR;
            Comprobante comprobante = Optional
                    .ofNullable(comprobanteRepository.obtenerComprobante(comprobantePK, codTipFiltro, numRucHeader))
                    .orElse(null);
        	if (Objects.nonNull(comprobante)) {
            comprobanteCDR = compFestoreRepository.findComprobanteCDRService(numRuc, numCpe, codCpe, numSerieCpe);
        	}else{
                comprobanteCDR = compFestoreRepository.findComprobanteCDRResumenService(numRuc, numCpe, codCpe, numSerieCpe);
            }
            String fileName = numRuc + "-" + codCpe
                    + "-" + numSerieCpe + "-"
                    + numCpe + ".zip";

            byte[] byteArray = comprobanteCDR.arcInput;

            File newFileXml = new File(fileName);
            FileUtils.writeByteArrayToFile(newFileXml, byteArray);
            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
                    "XmlComprobanteServiceImpl.recuperaComprobanteCdrService - FIN");
            return newFileXml;

        
        } catch (Exception e) {
            return null;
        }
    }


    private Comprobantes llenarDatosFacturaBoleta(Comprobante comprobante,List<DetalleComprobante> detalle
    		,List<Rubros> rubros,Document document,ParametrosSer parametrosSer
    		,Date fecEmision,List<DocumentoRelacionado> docRelacionado) 
    				throws IOException, ParserConfigurationException, SAXException, TransformerException {
        Comprobantes comprobanteIndivual = new Comprobantes();
        List<InformacionItems> informacionItems = new ArrayList<>();
        List<InformacionPercepcion> informacionPercepcions = new ArrayList<>();
        List<InformacionRetencion> informacionRetencions = new ArrayList<>();
        List<InformacionDetraccion> informacionDetraccionList = new ArrayList<>();
        DatosEmisor datosEmisor = new DatosEmisor();
        DatosReceptor datosReceptor = new DatosReceptor();
        boolean isPortal = false;
				if (Objects.isNull(document)) {
					try {

						Contribuyentes contribuyenteEmisor = contribuyenteRepository
								.obtenerContribuyente(comprobante.getComprobantePK().getNumRuc());
						String desDireccion = "-";
						String desUbigeo = "-";
						if (contribuyenteEmisor != null && contribuyenteEmisor.getUbigeo() != null) {
							desDireccion = String.join(" ", contribuyenteEmisor.getUbigeo().getDesNomVia(),
									contribuyenteEmisor.getUbigeo().getDesNumer1(),
									contribuyenteEmisor.getUbigeo().getDesNomZon(), contribuyenteEmisor.getUbigeo().getDesRefer1());
							
							desUbigeo = String.join("- ", contribuyenteEmisor.getUbigeo().getDesDepartamento(),
									contribuyenteEmisor.getUbigeo().getDesProvincia(), contribuyenteEmisor.getUbigeo().getDesDistrito());
						}

						datosEmisor.setDesDirEmis(desDireccion);
						datosEmisor.setDesDirEstEmis("");
						datosEmisor.setUbigeoEmis(desUbigeo);
						datosEmisor.setDesRazonSocialEmis(contribuyenteEmisor != null ? contribuyenteEmisor.getDesNombre() : "");
						datosEmisor.setDesNomComercialEmis(contribuyenteEmisor != null && contribuyenteEmisor.getCuentas() != null
								? contribuyenteEmisor.getCuentas().getNomComercial()
								: "");
						
						if (Constantes.COD_REGISTRO_UNICO.equals(comprobante.getCodDocIdeRecep())) {
							Contribuyentes contribuyenteReceptor = contribuyenteRepository
									.obtenerContribuyente(comprobante.getNumDocIdeRecep());

							String desDireccionRecep = "-";

							if (contribuyenteReceptor != null && contribuyenteReceptor.getUbigeo() != null) {
								desDireccionRecep = String.join(" ", contribuyenteReceptor.getUbigeo().getDesNomVia(),
										contribuyenteReceptor.getUbigeo().getDesNumer1(),
										contribuyenteReceptor.getUbigeo().getDesNomZon(), contribuyenteReceptor.getUbigeo().getDesRefer1());
							}

							datosReceptor
									.setDesRazonSocialRecep(contribuyenteReceptor != null ? contribuyenteReceptor.getDesNombre() : "");
							datosReceptor.setDirDetCliente(desDireccionRecep);
						} else {
							PersonasNaturales PersonasNaturales = personasNaturalesRepository
									.buscarContribuyentePorTipDocideNumDocide(comprobante.getCodDocIdeRecep(),
											comprobante.getNumDocIdeRecep());
							
							String desRazonRecep = "-";
							if (PersonasNaturales != null) {
								desRazonRecep = String.join(" ",
										PersonasNaturales.getApePaterno(),
										PersonasNaturales.getApeMaterno(),
										PersonasNaturales.getNomPerNat());
							}
							datosReceptor.setDesRazonSocialRecep(desRazonRecep);
							datosReceptor.setDirDetCliente(PersonasNaturales != null ? PersonasNaturales.getDesDir() : "");
						}
					} catch (Exception e) {
						utilLog.imprimirLog(ConstantesUtils.LEVEL_ERROR, "Error al obtener datos del emisor/receptor: " + e.getMessage());
					}

				} else {
					datosEmisor.setDesDirEmis(obtenerValorNodeDireccion(document, comprobante.getComprobantePK().getCodCpe()));
					String dirEstEmisor = obtenerValorNodeDirecEstEmisor(document);
					if (!dirEstEmisor.isEmpty()) {
						datosEmisor.setDesDirEstEmis(dirEstEmisor.trim());
					}
					datosEmisor.setUbigeoEmis(obtenerValorUbigeo(document, comprobante.getComprobantePK().getCodCpe()));
					datosEmisor.setDesRazonSocialEmis(obtenerValorNodeRazonSocial(document, comprobante.getComprobantePK().getCodCpe()));
                                        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "datosEmisor.getDesRazonSocialEmis===="+datosEmisor.getDesRazonSocialEmis());
					datosEmisor.setDesNomComercialEmis(
							obtenerValorNodeRazonComercial(document, comprobante.getComprobantePK().getCodCpe()));
					
					datosReceptor.setDesRazonSocialRecep(obtenerValorNodeRazonSocialReceptor(document,comprobante.getComprobantePK().getCodCpe()));
                                        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"datosEmisor.getDesRazonSocialEmis===="+datosReceptor.getDesRazonSocialRecep());
					datosReceptor.setDirDetCliente(obtenerValorNodeDirecReceptor(document,comprobante.getComprobantePK().getCodCpe()));
				}
				
        datosEmisor.setNumRuc(comprobante.getComprobantePK().getNumRuc());
        
        datosReceptor.setCodDocIdeRecep(comprobante.getCodDocIdeRecep());
        datosReceptor.setNumDocIdeRecep(comprobante.getNumDocIdeRecep());
        datosReceptor.setDirDetRecepFactura(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_989).isEmpty() ? ObtenerValorLugarRecep(document): obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_989));
        datosReceptor.setDirDetRecepBoleta(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_989).isEmpty() ? ObtenerValorLugarRecep(document): obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_989));
        datosReceptor.setDirDetVendedor(obtenerDatosDireccion(rubros, comprobante.getComprobantePK(), 21));
        datosReceptor.setDersLugarOperacion(obtenerDatosDireccion(rubros, comprobante.getComprobantePK(), 22));
        
        comprobanteIndivual.setDatosEmisor(datosEmisor);
        comprobanteIndivual.setDatosReceptor(datosReceptor);

        comprobanteIndivual.setNumSerie(comprobante.getComprobantePK().getNumSerieCpe());
        comprobanteIndivual.setCodCpe(comprobante.getComprobantePK().getCodCpe());

        comprobanteIndivual.setNumCpe(comprobante.getComprobantePK().getNumCpe());
        comprobanteIndivual.setCodMoneda(comprobante.getCodMoneda());
        comprobanteIndivual.setPlacaVehicular(obtenerValorPlacaFactura(document));
        if (!comprobante.getCodDocIdeRecep().isEmpty() && !comprobante.getCodDocIdeRecep().equals("-")) {
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(comprobante.getCodDocIdeRecep().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

        } else {
        		comprobanteIndivual.setDesTipoCpe("-");
        }
        if("00".endsWith(comprobante.getCodDocIdeRecep().trim()) || "0".endsWith(comprobante.getCodDocIdeRecep().trim())){
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
    	}
        comprobanteIndivual.setDesMoneda(parametrosSer.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(comprobante.getCodMoneda())).findFirst().map(TiposMoneda::getDesMoneda).orElse("-"));
        comprobanteIndivual.setDesSimbolo(parametrosSer.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(comprobante.getCodMoneda())).findFirst().map(TiposMoneda::getCodSimbolo).orElse("-"));
        comprobanteIndivual.setFecEmision(comprobante.getFecEmision());
        comprobanteIndivual.setFecRegistro(comprobante.getFecEmision());
        comprobanteIndivual.setFecVencimiento(obteneFechaRubro(rubros, RubrosEnum.RUBRO_158));

        comprobanteIndivual.setCodTipTransaccion(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_986));
        comprobanteIndivual.setIndEstadoCpe(comprobante.getIndEstado());
        comprobanteIndivual.setIndProcedencia(comprobante.getIndProcedencia());
        comprobanteIndivual.setIndTituloGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_131).compareTo(BigDecimal.ZERO) > 0 ? "1" : "0");
        comprobanteIndivual.setMtoVentaOpGratuita(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_131));
        comprobanteIndivual.setDesMtoTotalLetras(Numero.numerosALetras(comprobante.getMtoImporteTotal()) + " "
                + ComprobanteUtilBean.getDescripcionMoneda(comprobante.getCodMoneda()));
        comprobanteIndivual.setIndTaxFree(ComprobanteUtilBean.validarTaxFree(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_194)));
        comprobanteIndivual.setIndItinerante("");
        comprobanteIndivual.setDesObservacion(comprobante.getDesObservacion());
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "RESULTADO DEL DETALLE COMP" + detalle);
        utilLog.imprimirLog(ConstantesUtils.LEVEL_INFO, "RESULTADO DEL DETALLE COMP INFO" + detalle);
        
        
        

        if (comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_PORTAL) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_EMPRENDER) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_SUNAT) 
        		|| comprobante.getIndProcedencia().equals(Constantes.APP_PERSONAS)) {
        		isPortal = true;
            ProcedenciaIndividual procedenciaIndividual = new ProcedenciaIndividual();
            procedenciaIndividual.setMtoSubTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_101));
            procedenciaIndividual.setMtoOpExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
            procedenciaIndividual.setMtoOpGravado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
            procedenciaIndividual.setMtoOpInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
            //procedenciaIndividual.setMtoAnticipos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_960));
            procedenciaIndividual.setMtoAnticipos(obtenerMontoAnticipoXMLPortal(document));
            procedenciaIndividual.setMtoDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
            procedenciaIndividual.setMtoISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
            
            if (comprobante.getMtoTotalVenta().max(BigDecimal.ZERO)!= BigDecimal.ZERO) {
            	procedenciaIndividual.setMtoValorVenta(comprobante.getMtoTotalVenta().max(BigDecimal.ZERO));
	       	}else{
	       		procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110).max(BigDecimal.ZERO));
	      	}

            if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953) != BigDecimal.ZERO) {
                procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952).max(BigDecimal.ZERO));
                procedenciaIndividual.setMtoIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953).max(BigDecimal.ZERO));
            } else {
            	 if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104) != BigDecimal.ZERO) {
            		 procedenciaIndividual.setMtoIGV(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104).max(BigDecimal.ZERO));
            	 }else{
            		 procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv().max(BigDecimal.ZERO));
            	 }
            }

            procedenciaIndividual.setMtoOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
            procedenciaIndividual.setMtoOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
            procedenciaIndividual.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
            procedenciaIndividual.setMtoICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
            procedenciaIndividual.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
            comprobanteIndivual.setProcedenciaIndividual(procedenciaIndividual);
        }
        if (comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_GEM) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_OSE) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_BAJA_O_NULA_CONECTIVIDAD)) {

            ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();
            procedenciaMasiva.setMtoDtoGlobalAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_958));
            procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
            procedenciaMasiva.setMtoTotalValVentaInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
            procedenciaMasiva.setMtoTotalValVentaExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
            procedenciaMasiva.setMtoTotalValVentaGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_137));
            procedenciaMasiva.setMtoTotalValVentaExportacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_142));
            procedenciaMasiva.setMtoDtoGlobalNoAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_959));
            procedenciaMasiva.setMtoTotalDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
            procedenciaMasiva.setMtoSumOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
//            procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
            procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoOtrosCargosXMLGEM(document));
            procedenciaMasiva.setMtoSumISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
            if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953) != BigDecimal.ZERO) {
                procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952));
                procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953));
            } else {
              procedenciaMasiva.setMtoSumIGV(comprobante.getMtoTotalIgv());
            }
            procedenciaMasiva.setMtoSumICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
            procedenciaMasiva.setMtoTotalAnticipo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
            procedenciaMasiva.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
            procedenciaMasiva.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
            comprobanteIndivual.setProcedenciaMasiva(procedenciaMasiva);
        }
        if (comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_CONTINGENCIA_GEM_PORTAL)) {
            ComprobanteXML comprobanteXml = null;
            comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(comprobante.getComprobantePK().getNumRuc(), String.valueOf(comprobante.getComprobantePK().getNumCpe()), comprobante.getComprobantePK().getCodCpe(), comprobante.getComprobantePK().getNumSerieCpe());
            if (Objects.nonNull(comprobanteXml)) {
                ProcedenciaIndividual procedenciaIndividual = new ProcedenciaIndividual();
                procedenciaIndividual.setMtoSubTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_101));
                procedenciaIndividual.setMtoOpExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
                procedenciaIndividual.setMtoOpGravado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
                procedenciaIndividual.setMtoOpInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
//                procedenciaIndividual.setMtoAnticipos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_960));
                procedenciaIndividual.setMtoAnticipos(obtenerMontoAnticipoXMLPortal(document));
                procedenciaIndividual.setMtoDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
                procedenciaIndividual.setMtoISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
                if (comprobante.getMtoTotalVenta().max(BigDecimal.ZERO)!= BigDecimal.ZERO) {
                	procedenciaIndividual.setMtoValorVenta(comprobante.getMtoTotalVenta().max(BigDecimal.ZERO));
    	       	}else{
    	       		procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110).max(BigDecimal.ZERO));
    	      	}
                if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953) != BigDecimal.ZERO) {
                    procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952).max(BigDecimal.ZERO));
                    procedenciaIndividual.setMtoIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953).max(BigDecimal.ZERO));
                } else {
                    //procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv());
                    procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv().max(BigDecimal.ZERO));
                }

                procedenciaIndividual.setMtoOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
                procedenciaIndividual.setMtoOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
                procedenciaIndividual.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
                procedenciaIndividual.setMtoICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
                procedenciaIndividual.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
                comprobanteIndivual.setProcedenciaIndividual(procedenciaIndividual);
            } else {

                ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();
                comprobanteIndivual.setIndProcedencia(Constantes.PROCEDENCIA_GEM);//cuando es contingencia (4) pero es GEM, el indicador debe ser GEM(2)
                procedenciaMasiva.setMtoDtoGlobalAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_958));
                procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
                procedenciaMasiva.setMtoTotalValVentaInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
                procedenciaMasiva.setMtoTotalValVentaExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
                procedenciaMasiva.setMtoTotalValVentaGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_137));
                procedenciaMasiva.setMtoTotalValVentaExportacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_142));
                procedenciaMasiva.setMtoDtoGlobalNoAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_959));
                procedenciaMasiva.setMtoTotalDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
                procedenciaMasiva.setMtoSumOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
//                procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
                procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoOtrosCargosXMLGEM(document));
                procedenciaMasiva.setMtoSumISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));


                if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953) != BigDecimal.ZERO) {
                    procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952));
                    procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953));
                } else {
                  procedenciaMasiva.setMtoSumIGV(comprobante.getMtoTotalIgv());
                }
                procedenciaMasiva.setMtoSumICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
                procedenciaMasiva.setMtoTotalAnticipo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
                procedenciaMasiva.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
                procedenciaMasiva.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
                comprobanteIndivual.setProcedenciaMasiva(procedenciaMasiva);

            }

        }
        if (comprobanteIndivual.getCodCpe().equals(Constantes.COD_CPE_FACTURA)){
            if(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_986).equals(Constantes.FORMA_PAGO_CREDITO)){
                InformacionCredito informacionCredito = new InformacionCredito();
                informacionCredito.setMtoPagoPendiente(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_987));
                informacionCredito.setFecPlazoPago(obteneFechaRubro(rubros, RubrosEnum.RUBRO_995));
                List<InformacionCredito> informacionCreditoList = new ArrayList<>();

                informacionCredito.setNumCuotasList(obtenerCuotas(rubros));
                informacionCredito.setNumCuotas(informacionCredito.getNumCuotasList().size());
                informacionCreditoList.add(informacionCredito);
                comprobanteIndivual.setInformacionCreditos(informacionCreditoList);
            }
            //obtener credito
            
            DecimalFormat df = new DecimalFormat("#.00#####");
            
            //obtener detraccion
            InformacionDetraccion informacionDetraccion = new InformacionDetraccion();
            informacionDetraccion.setMtoDetraccion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_804));
            informacionDetraccion.setPorDetraccion(df.format(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_803)));
            informacionDetraccion.setDesLeyenda(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_2006));
            informacionDetraccion.setDesBienServicio(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_801) + "-" + ComprobanteUtilBean.obtenerDescCatalogo54(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_801)));
            informacionDetraccion.setMedioPago(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_805) + "-" + ComprobanteUtilBean.obtenerDescCatalogo59(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_805)));

            informacionDetraccion.setNroCuenta(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_802));
            informacionDetraccionList.add(informacionDetraccion);
            //obtener percepcion
            InformacionPercepcion informacionPercepcion = new InformacionPercepcion();
            informacionPercepcion.setMtoPercepcion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_941));
            informacionPercepcion.setMtoTotalPercepcion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_9999));
            informacionPercepcion.setDesLeyenda(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_2000));
            informacionPercepcions.add(informacionPercepcion);
            //obtener retencion
            InformacionRetencion informacionRetencion = new InformacionRetencion();
            informacionRetencion.setMtoRetencion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_992));
            informacionRetencion.setPorRetencion(ComprobanteUtilBean.convertirPorcentaje(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_991)));
            informacionRetencion.setDesBaseImponibleRetencion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_990));
            informacionRetencions.add(informacionRetencion);
            if (informacionDetraccion.getMtoDetraccion().intValue() > 0) {
                comprobanteIndivual.setInformacionDetraccions(informacionDetraccionList);
            }
            if (informacionPercepcion.getMtoPercepcion().intValue() > 0) {
                comprobanteIndivual.setInformacionPercepcions(informacionPercepcions);
            }
            if (informacionRetencion.getMtoRetencion().intValue() > 0) {
                comprobanteIndivual.setInformacionRetencions(informacionRetencions);
            }

        }
        
        if (comprobanteIndivual.getCodCpe().equals(Constantes.COD_CPE_BOLETA)){
        	InformacionPercepcion informacionPercepcion = new InformacionPercepcion();
            informacionPercepcion.setMtoPercepcion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_941));
            informacionPercepcion.setMtoTotalPercepcion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_9999));
            informacionPercepcion.setDesLeyenda(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_2000));
            informacionPercepcions.add(informacionPercepcion);
            if (informacionPercepcion.getMtoPercepcion().intValue() > 0) {
                comprobanteIndivual.setInformacionPercepcions(informacionPercepcions);
            }
        }
        
        List<InformacionItems> listItems = obtenerInformacionItems(document);
        for (DetalleComprobante det : detalle) {
            InformacionItems info = new InformacionItems();
            Optional<InformacionItems> item = listItems.stream().filter(infoItem -> 
            det.getDetalleComprobantePK().getNumFilaItem() == infoItem.getNumFila()).findFirst();
            
						if (item.isPresent()) {
							info.setCntItems(item.get().getCntItems());
							info.setMtoValUnitario(item.get().getMtoValUnitario());
							info.setMtoImpTotal(item.get().getMtoImpTotal());
						} else {
							info.setCntItems(det.getNumCantItem());
							info.setMtoValUnitario(
									obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_111, det.getDetalleComprobantePK().getNumFilaItem()));
							if (obtenerDescripcionRubroxitem(rubros, RubrosEnum.RUBRO_224,
									det.getDetalleComprobantePK().getNumFilaItem()).equals(Constantes.INDICADOR_ANTICIPO)) {
								info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_233,
										det.getDetalleComprobantePK().getNumFilaItem()));
							} else {
								if ((obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
										det.getDetalleComprobantePK().getNumFilaItem())).compareTo(BigDecimal.ZERO) > 0) {
									info.setMtoImpTotal(obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
											det.getDetalleComprobantePK().getNumFilaItem()));
								} else {
									info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_134,
											det.getDetalleComprobantePK().getNumFilaItem()));
								}
							}
						}
            info.setNumFila(det.getDetalleComprobantePK().getNumFilaItem());
            
            info.setCodUnidadMedida(det.getCodUnidadItem());
            String desUnidad=parametrosSer.getTiposUnidades().stream().filter(tiposUnidades -> tiposUnidades.getCodUnidad().equals(det.getCodUnidadItem())).findFirst().map(TiposUnidades::getDesUnidad).orElse("-");
            String[] desUnidadArray= desUnidad.split("\\(");
            String desUnidadPrimerResultado = desUnidadArray[0].trim();
            info.setDesUnidadMedida(desUnidadPrimerResultado);
            info.setDesCodigo(det.getCodDetalleItem());
            info.setDesItem(det.getDesDetalleItem());
            info.setMtoDesc(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_112, det.getDetalleComprobantePK().getNumFilaItem()));
            info.setMtoICBPER(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_985, det.getDetalleComprobantePK().getNumFilaItem()));
            informacionItems.add(info);
        }
        comprobanteIndivual.setInformacionItems(informacionItems);
        
        //obtener informacion relaciona
        List<InformacionRelacionada> informacionRelacionadas = new ArrayList<>();
        InformacionRelacionada informacionRelacionada = new InformacionRelacionada();
        if (!obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_160).isEmpty() ||
                !obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_161).isEmpty() ||
                !obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_162).isEmpty() ||
                !obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_163).isEmpty() ||
                !obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_164).isEmpty()

        ) {
            informacionRelacionada.setNroExpediente(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_160));
            informacionRelacionada.setCodUnidadEjecutora(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_162));
            informacionRelacionada.setNroOrdenCompra(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_161));
            informacionRelacionada.setNumContrato(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_164));
            informacionRelacionada.setNumProcesoSeleccion(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_163));
            informacionRelacionadas.add(informacionRelacionada);
            comprobanteIndivual.setInformacionRelacionada(informacionRelacionadas);
        }
				List<InformacionDocumentosRelacionados> infoDocsRel = obtenerDocsRel(document);
				List<InformacionDocumentosRelacionados> informacionDocumentosRelacionados = new ArrayList<>();
				List<String> docRelAnticipo = Arrays.asList("01","02","03");
				 
				for (DocumentoRelacionado documentoRel : docRelacionado) {
					String codAgrup = Arrays.asList(LIST_AGRUP_REL_FACBOL_CATALOGO1)
							.contains(documentoRel.getCodRelacion())
									? Constantes.COD_AGRUP_REL_CATALOGO1
									: Arrays.asList(LIST_AGRUP_REL_FACBOL_CATALOGO12).contains(documentoRel.getCodRelacion())
											? Constantes.COD_AGRUP_REL_CATALOGO12 : "-";
					boolean isExistsDocRel = docRelAnticipo.contains(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
					if (parametrosSer.getTiposComprobanteRelacionado().stream()
							.anyMatch(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
									.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
									&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup)&& !isExistsDocRel)) {
						
						InformacionDocumentosRelacionados infdoc = new InformacionDocumentosRelacionados();
						
						Optional<InformacionDocumentosRelacionados> iDoc = infoDocsRel.stream()
								.filter(iDocRel -> (iDocRel.getNumSerieDocRel().equals("-")? true:iDocRel.getNumSerieDocRel().trim().equals(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel().trim()))
								&& iDocRel.getNumDocRel().trim().equals(documentoRel.getDocumentoRelacionadoPK().getNumDocRel()))
								.findFirst();
						if(isPortal && iDoc.isPresent() && iDoc.get().getDesCpeRel()!=null) {
							infdoc.setCodCpeRel(iDoc.get().getCodCpeRel());
							infdoc.setNumDocRel(iDoc.get().getNumDocRel());
							infdoc.setNumSerieDocRel(iDoc.get().getNumSerieDocRel());
							
							if(iDoc.get().getDesCpeRel().isEmpty() || iDoc.get().getDesCpeRel()==null){
								infdoc.setDesCpeRel(parametrosSer.getTiposComprobanteRelacionado().stream()
										.filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
												.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
												&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))
										.findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
							}else{
								infdoc.setDesCpeRel(iDoc.get().getDesCpeRel());
							}

							infdoc.setCodAgrupProc(codAgrup);
						} else {
							infdoc.setCodCpeRel(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
							infdoc.setNumDocRel(documentoRel.getDocumentoRelacionadoPK().getNumDocRel());
							infdoc.setNumSerieDocRel(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel());
							infdoc.setDesCpeRel(parametrosSer.getTiposComprobanteRelacionado().stream()
									.filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
											.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
											&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))
									.findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
							infdoc.setCodAgrupProc(codAgrup);
						}
						
						informacionDocumentosRelacionados.add(infdoc);

					}

				}
        DocumentoRelacionadoPK docRelacionadoPk = new DocumentoRelacionadoPK();
        docRelacionadoPk.setNumDocRel(String.valueOf(comprobante.getComprobantePK().getNumCpe()));
        docRelacionadoPk.setCodDocRel(comprobante.getComprobantePK().getCodCpe());
        docRelacionadoPk.setNumSerieDocRel(comprobante.getComprobantePK().getNumSerieCpe());
        docRelacionadoPk.setNumRucRel(comprobante.getComprobantePK().getNumRuc());

        List<DocumentoRelacionado> notas = Optional
                .ofNullable(docRelacionadoRepository.obtenerNotas(docRelacionadoPk))
                .orElse(null);
        List<InformacionNotas> lstInformacionNotas = new ArrayList<>();
        int contador = 1;

        if (Objects.nonNull(notas)) {
            for (DocumentoRelacionado documentoRel : notas) {
                InformacionNotas informacionNota = new InformacionNotas();
                informacionNota.setNumCorrel(contador);
                informacionNota.setFecEmision(documentoRel.getFecEmision());
                informacionNota.setMtoTotal(documentoRel.getMtoTotal());
                informacionNota.setCodTipNota(ComprobanteUtilBean.obtenerTipoNota(documentoRel.getDocumentoRelacionadoPK().getCodCpe()));
                informacionNota.setNumCpe(documentoRel.getDocumentoRelacionadoPK().getNumCpe());
                informacionNota.setNumSerie(documentoRel.getDocumentoRelacionadoPK().getNumSerieCpe());
              informacionNota.setCodTipCpe(documentoRel.getCodTipoCpe());
                lstInformacionNotas.add(informacionNota);
                contador = contador + 1;
            }
            if (!notas.isEmpty()) {
                comprobanteIndivual.setInformacionNotas(lstInformacionNotas);
            }
        }


        if (!informacionDocumentosRelacionados.isEmpty()) {
            comprobanteIndivual.setInformacionDocumentosRelacionados(informacionDocumentosRelacionados);

        }
        return  comprobanteIndivual;

    }

    private Comprobantes llenarDatosNotas(Comprobante comprobante,List<DetalleComprobante> detalle
    		,List<Rubros> rubros,Document document,ParametrosSer parametrosSer
    		,Date fecEmision,List<DocumentoRelacionado> docRelacionado) 
    				throws IOException, ParserConfigurationException, SAXException, TransformerException {
        Comprobantes comprobanteIndivual= new Comprobantes();
        List<InformacionItems> informacionItems = new ArrayList<>();
        DatosEmisor datosEmisor = new DatosEmisor();
        DatosReceptor datosReceptor = new DatosReceptor();
        
        if (Objects.isNull(document)) {
			try {

				Contribuyentes contribuyenteEmisor = contribuyenteRepository
						.obtenerContribuyente(comprobante.getComprobantePK().getNumRuc());
				String desDireccion = "-";
				String desUbigeo = "-";
				if (contribuyenteEmisor != null && contribuyenteEmisor.getUbigeo() != null) {
					desDireccion = String.join(" ", contribuyenteEmisor.getUbigeo().getDesNomVia(),
							contribuyenteEmisor.getUbigeo().getDesNumer1(),
							contribuyenteEmisor.getUbigeo().getDesNomZon(), contribuyenteEmisor.getUbigeo().getDesRefer1());
					
					desUbigeo = String.join("- ", contribuyenteEmisor.getUbigeo().getDesDepartamento(),
							contribuyenteEmisor.getUbigeo().getDesProvincia(), contribuyenteEmisor.getUbigeo().getDesDistrito());
				}

				datosEmisor.setDesDirEmis(desDireccion);
				datosEmisor.setDesDirEstEmis("");
				datosEmisor.setUbigeoEmis(desUbigeo);
				datosEmisor.setDesRazonSocialEmis(contribuyenteEmisor != null ? contribuyenteEmisor.getDesNombre() : "");
				datosEmisor.setDesNomComercialEmis(contribuyenteEmisor != null && contribuyenteEmisor.getCuentas() != null
						? contribuyenteEmisor.getCuentas().getNomComercial()
						: "");
				
				if (Constantes.COD_REGISTRO_UNICO.equals(comprobante.getCodDocIdeRecep())) {
					Contribuyentes contribuyenteReceptor = contribuyenteRepository
							.obtenerContribuyente(comprobante.getNumDocIdeRecep());

					String desDireccionRecep = "-";

					if (contribuyenteReceptor != null && contribuyenteReceptor.getUbigeo() != null) {
						desDireccionRecep = String.join(" ", contribuyenteReceptor.getUbigeo().getDesNomVia(),
								contribuyenteReceptor.getUbigeo().getDesNumer1(),
								contribuyenteReceptor.getUbigeo().getDesNomZon(), contribuyenteReceptor.getUbigeo().getDesRefer1());
					}

					datosReceptor
							.setDesRazonSocialRecep(contribuyenteReceptor != null ? contribuyenteReceptor.getDesNombre() : "");
					datosReceptor.setDirDetCliente(desDireccionRecep);
				} else {
					PersonasNaturales PersonasNaturales = personasNaturalesRepository
							.buscarContribuyentePorTipDocideNumDocide(comprobante.getCodDocIdeRecep(),
									comprobante.getNumDocIdeRecep());
					
					String desRazonRecep = "-";
					if (PersonasNaturales != null) {
						desRazonRecep = String.join(" ",
								PersonasNaturales.getApePaterno(),
								PersonasNaturales.getApeMaterno(),
								PersonasNaturales.getNomPerNat());
					}
					datosReceptor.setDesRazonSocialRecep(desRazonRecep);
					datosReceptor.setDirDetCliente(PersonasNaturales != null ? PersonasNaturales.getDesDir() : "");
				}
			} catch (Exception e) {
				utilLog.imprimirLog(ConstantesUtils.LEVEL_ERROR, "Error al obtener datos del emisor/receptor: " + e.getMessage());
			}

		} else {
			datosEmisor.setDesDirEmis(obtenerValorNodeDireccion(document, comprobante.getComprobantePK().getCodCpe()));
			String dirEstEmisor = obtenerValorNodeDirecEstEmisor(document);
			if (!dirEstEmisor.isEmpty()) {
				datosEmisor.setDesDirEstEmis(dirEstEmisor.trim());
			}
			datosEmisor.setUbigeoEmis(obtenerValorUbigeo(document, comprobante.getComprobantePK().getCodCpe()));
			datosEmisor.setDesRazonSocialEmis(obtenerValorNodeRazonSocial(document, comprobante.getComprobantePK().getCodCpe()));
                                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "datosEmisor.getDesRazonSocialEmis===="+datosEmisor.getDesRazonSocialEmis());
			datosEmisor.setDesNomComercialEmis(
					obtenerValorNodeRazonComercial(document, comprobante.getComprobantePK().getCodCpe()));
			
			datosReceptor.setDesRazonSocialRecep(obtenerValorNodeRazonSocialReceptor(document,comprobante.getComprobantePK().getCodCpe()));
                                utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"datosEmisor.getDesRazonSocialEmis===="+datosReceptor.getDesRazonSocialRecep());
			datosReceptor.setDirDetCliente(obtenerValorNodeDirecReceptor(document,comprobante.getComprobantePK().getCodCpe()));
		}
        
        boolean isPortal=false;
        
        datosEmisor.setNumRuc(comprobante.getComprobantePK().getNumRuc());
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"datosEmisor.getDesRazonSocialEmis===="+datosEmisor.getDesRazonSocialEmis());

        datosReceptor.setCodDocIdeRecep(comprobante.getCodDocIdeRecep());
        datosReceptor.setNumDocIdeRecep(comprobante.getNumDocIdeRecep());
        datosReceptor.setDirDetRecepFactura(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_989).isEmpty() ? ObtenerValorLugarRecep(document): obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_989));
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "datosReceptor.getDesRazonSocialEmis===="+datosReceptor.getDesRazonSocialRecep());
        
        datosReceptor.setDirDetVendedor(obtenerDatosDireccion(rubros, comprobante.getComprobantePK(), 21));
        datosReceptor.setDersLugarOperacion(obtenerDatosDireccion(rubros, comprobante.getComprobantePK(), 22));
        comprobanteIndivual.setDatosEmisor(datosEmisor);
        comprobanteIndivual.setDatosReceptor(datosReceptor);

        comprobanteIndivual.setNumSerie(comprobante.getComprobantePK().getNumSerieCpe());
        comprobanteIndivual.setCodCpe(comprobante.getComprobantePK().getCodCpe());

        comprobanteIndivual.setNumCpe(comprobante.getComprobantePK().getNumCpe());
        comprobanteIndivual.setCodMoneda(comprobante.getCodMoneda());
        if (!comprobante.getCodDocIdeRecep().isEmpty() && !comprobante.getCodDocIdeRecep().equals("-")) {
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(comprobante.getCodDocIdeRecep().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

        } else {
        		comprobanteIndivual.setDesTipoCpe("-");
        }
        if("00".endsWith(comprobante.getCodDocIdeRecep().trim()) || "0".endsWith(comprobante.getCodDocIdeRecep().trim())){
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
    	}
        comprobanteIndivual.setDesMoneda(parametrosSer.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(comprobante.getCodMoneda())).findFirst().map(TiposMoneda::getDesMoneda).orElse("-"));
        comprobanteIndivual.setDesSimbolo(parametrosSer.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(comprobante.getCodMoneda())).findFirst().map(TiposMoneda::getCodSimbolo).orElse("-"));
        comprobanteIndivual.setFecEmision(comprobante.getFecEmision());
        comprobanteIndivual.setFecRegistro(comprobante.getFecEmision());
        comprobanteIndivual.setFecVencimiento(obteneFechaRubro(rubros, RubrosEnum.RUBRO_158));

        comprobanteIndivual.setCodTipTransaccion(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_986));
        comprobanteIndivual.setIndEstadoCpe(comprobante.getIndEstado());
        comprobanteIndivual.setIndProcedencia(comprobante.getIndProcedencia());
        comprobanteIndivual.setIndTituloGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_131).compareTo(BigDecimal.ZERO) > 0 ? "1" : "0");
        comprobanteIndivual.setMtoVentaOpGratuita(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_131));
        
        comprobanteIndivual.setDesMtoTotalLetras(Numero.numerosALetras(comprobante.getMtoImporteTotal()) + " "
                + ComprobanteUtilBean.getDescripcionMoneda(comprobante.getCodMoneda()));
        if(Constantes.PROCEDENCIA_GEM.equals(comprobante.getIndProcedencia()) || "-".equals(comprobante.getDesObservacion()) || comprobante.getDesObservacion().isEmpty() || comprobante.getDesObservacion()==null) {
        	comprobanteIndivual.setDesObservacion(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_193));
        }else {
        	comprobanteIndivual.setDesObservacion(comprobante.getDesObservacion());
        }
        
        if (comprobanteIndivual.getCodCpe().equals(Constantes.COD_CPE_NOTA_CREDITO)) {
            if (!docRelacionado.isEmpty()) {
                Optional<DocumentoRelacionado> docrelacion = docRelacionado.stream()
                		.filter(documentoRelacionado -> documentoRelacionado
                				.getDocumentoRelacionadoPK().getCodDocRel()
                				.equals(Constantes.COD_CPE_BOLETA) 
                				|| documentoRelacionado.getDocumentoRelacionadoPK().getCodDocRel()
                				.equals(Constantes.COD_CPE_FACTURA)).findFirst();
                if (docrelacion.isPresent()) {
                    comprobanteIndivual.setNumCpeRel(docrelacion.get().getDocumentoRelacionadoPK().getNumDocRel().isEmpty() ? 0 : Integer.parseInt(docrelacion.get().getDocumentoRelacionadoPK().getNumDocRel()));
                    comprobanteIndivual.setNumSerieRel(docrelacion.get().getDocumentoRelacionadoPK().getNumSerieDocRel());
                    comprobanteIndivual.setCodDocRel(docrelacion.get().getDocumentoRelacionadoPK().getCodDocRel());
                }
            } else {
                comprobanteIndivual.setNumCpeRel(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_217).isEmpty() ? 0 : Integer.parseInt(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_217)));
                comprobanteIndivual.setNumSerieRel(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_215));
                comprobanteIndivual.setCodDocRel(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_213));
            }
        }
        if (comprobanteIndivual.getCodCpe().equals(Constantes.COD_CPE_NOTA_DEBITO)) {
            if (!docRelacionado.isEmpty()) {
                Optional<DocumentoRelacionado> docrelacion = docRelacionado.stream().filter(documentoRelacionado -> documentoRelacionado.getDocumentoRelacionadoPK().getCodDocRel().equals(Constantes.COD_CPE_BOLETA) || documentoRelacionado.getDocumentoRelacionadoPK().getCodDocRel().equals(Constantes.COD_CPE_FACTURA)).findFirst();
                if (docrelacion.isPresent()) {
                    comprobanteIndivual.setNumCpeRel(docrelacion.get().getDocumentoRelacionadoPK().getNumDocRel().isEmpty() ? 0 : Integer.parseInt(docrelacion.get().getDocumentoRelacionadoPK().getNumDocRel()));
                    comprobanteIndivual.setNumSerieRel(docrelacion.get().getDocumentoRelacionadoPK().getNumSerieDocRel());
                    comprobanteIndivual.setCodDocRel(docrelacion.get().getDocumentoRelacionadoPK().getCodDocRel());
                }
            } else {
                comprobanteIndivual.setNumCpeRel(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_216).isEmpty() ? 0 : Integer.parseInt(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_216)));
                comprobanteIndivual.setNumSerieRel(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_214));
                comprobanteIndivual.setCodDocRel(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_212));

            }


        }
        
        if (comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_PORTAL) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_EMPRENDER) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_SUNAT) 
        		|| comprobante.getIndProcedencia().equals(Constantes.APP_PERSONAS)) {
        	isPortal=true;
        }
        
        if (comprobante.getCodTipoCpe().equals(Constantes.COD_TIP_NOTA_DESCUENTO_GLOBAL) || (comprobante.getCodTipoCpe().equals(Constantes.COD_TIP_NOTA_INTERES_MORA)) ||
        		(comprobante.getCodTipoCpe().equals(Constantes.COD_TIP_NOTA_PENALIDAD)) || 
        		(Arrays.asList(COD_PROCEDENCIAS_GEM).contains(comprobante.getIndProcedencia()) && 
        		 Arrays.asList(COD_NOTA_DEBITO_GEM).contains(comprobante.getCodTipoCpe()) && Constantes.COD_CPE_NOTA_DEBITO.equals(comprobante.getComprobantePK().getCodCpe()))
        		) {
        	
        	if(detalle.isEmpty()){
	            InformacionItems info = new InformacionItems();
	            info.setCntItems(BigDecimal.ZERO);
	            info.setCodUnidadMedida("-");
	            info.setDesUnidadMedida("-");
	            info.setDesCodigo("-");
	            info.setDesItem(comprobante.getDesObservacion());
	            info.setMtoValUnitario(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_130));
	
	
	            info.setMtoICBPER(BigDecimal.ZERO);
	            info.setMtoImpTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
	            info.setMtoDesc(BigDecimal.ZERO);
	
	            informacionItems.add(info);
	            comprobanteIndivual.setInformacionItems(informacionItems);
        	}else{

        		List<InformacionItems> listItems = obtenerInformacionItemsNotasDebito(document);
                for (DetalleComprobante det : detalle) {
                    InformacionItems info = new InformacionItems();
                    Optional<InformacionItems> item = listItems.stream().filter(infoItem -> 
                    det.getDetalleComprobantePK().getNumFilaItem() == infoItem.getNumFila()).findFirst();
                    
        						if (item.isPresent()) {
        							info.setCntItems(item.get().getCntItems());
        							info.setMtoValUnitario(item.get().getMtoValUnitario());
        							info.setMtoImpTotal(item.get().getMtoImpTotal());
        						} else {
        							info.setCntItems(det.getNumCantItem());
        							info.setMtoValUnitario(
        									obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_111, det.getDetalleComprobantePK().getNumFilaItem()));
        							if (obtenerDescripcionRubroxitem(rubros, RubrosEnum.RUBRO_224,
        									det.getDetalleComprobantePK().getNumFilaItem()).equals(Constantes.INDICADOR_ANTICIPO)) {
        								info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_233,
        										det.getDetalleComprobantePK().getNumFilaItem()));
        							} else {
        								if ((obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
        										det.getDetalleComprobantePK().getNumFilaItem())).compareTo(BigDecimal.ZERO) > 0) {
        									info.setMtoImpTotal(obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
        											det.getDetalleComprobantePK().getNumFilaItem()));
        								} else {
        									info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_134,
        											det.getDetalleComprobantePK().getNumFilaItem()));
        								}
        							}
        						}
                    info.setNumFila(det.getDetalleComprobantePK().getNumFilaItem());
                    
                    info.setCodUnidadMedida(det.getCodUnidadItem());
                    String desUnidad=parametrosSer.getTiposUnidades().stream().filter(tiposUnidades -> tiposUnidades.getCodUnidad().equals(det.getCodUnidadItem())).findFirst().map(TiposUnidades::getDesUnidad).orElse("-");
                    String[] desUnidadArray= desUnidad.split("\\(");
                    String desUnidadPrimerResultado = desUnidadArray[0].trim();
                    info.setDesUnidadMedida(desUnidadPrimerResultado);
                    info.setDesCodigo(det.getCodDetalleItem());
                    info.setDesItem(det.getDesDetalleItem());
                    info.setMtoDesc(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_112, det.getDetalleComprobantePK().getNumFilaItem()));
                    info.setMtoICBPER(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_985, det.getDetalleComprobantePK().getNumFilaItem()));
                    informacionItems.add(info);
                }
                comprobanteIndivual.setInformacionItems(informacionItems);
        	}
        } else {
            //obtener rubro documento relacionado
            RubrosPK rubrosRelPk = new RubrosPK();
            rubrosRelPk.setNumSerieCpe(comprobanteIndivual.getNumSerieRel());
            rubrosRelPk.setCodCpe(comprobanteIndivual.getCodDocRel());
            rubrosRelPk.setNumRuc(comprobanteIndivual.getDatosEmisor().getNumRuc());
            rubrosRelPk.setNumCpe(comprobanteIndivual.getNumCpeRel());
            
            List<Rubros> rubrosRel = Optional
                    .ofNullable(rubrosRepository.obtenerRubros(rubrosRelPk))
                    .orElse(null);
            if(rubrosRel==null || rubrosRel.isEmpty()){
            rubrosRel = Optional
                    .ofNullable(rubrosRepositoryhist.obtenerRubros(rubrosRelPk))
                    .orElse(null);
            }
            List<InformacionItems> listItems = obtenerInformacionItemsNotasCredito(document);
            
            for (DetalleComprobante det : detalle) {
                InformacionItems info = new InformacionItems();
                Optional<InformacionItems> item = listItems.stream().filter(infoItem -> 
                det.getDetalleComprobantePK().getNumFilaItem() == infoItem.getNumFila()).findFirst();
                
                if(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_111, det.getDetalleComprobantePK().getNumFilaItem()).compareTo(BigDecimal.ZERO)==0){

								if (item.isPresent()) {
									info.setCntItems(item.get().getCntItems());
									info.setMtoValUnitario(item.get().getMtoValUnitario());
									info.setMtoImpTotal(item.get().getMtoImpTotal());
								} else {
									info.setCntItems(det.getNumCantItem());
									info.setMtoValUnitario(
											obtenerMontoRubroxitem(rubrosRel, RubrosEnum.RUBRO_111, det.getDetalleComprobantePK().getNumFilaItem()));
									if (obtenerDescripcionRubroxitem(rubrosRel, RubrosEnum.RUBRO_224,
											det.getDetalleComprobantePK().getNumFilaItem()).equals(Constantes.INDICADOR_ANTICIPO)) {
										info.setMtoImpTotal(obtenerMontoRubroxitem(rubrosRel, RubrosEnum.RUBRO_233,
												det.getDetalleComprobantePK().getNumFilaItem()));
									} else {
										if ((obtenerMontoDescRubroxItem(rubrosRel, RubrosEnum.RUBRO_119,
												det.getDetalleComprobantePK().getNumFilaItem())).compareTo(BigDecimal.ZERO) > 0) {
											info.setMtoImpTotal(obtenerMontoDescRubroxItem(rubrosRel, RubrosEnum.RUBRO_119,
													det.getDetalleComprobantePK().getNumFilaItem()));
										} else {
											info.setMtoImpTotal(obtenerMontoRubroxitem(rubrosRel, RubrosEnum.RUBRO_134,
													det.getDetalleComprobantePK().getNumFilaItem()));
										}
									}
								}
			        info.setNumFila(det.getDetalleComprobantePK().getNumFilaItem());
			        
			        info.setCodUnidadMedida(det.getCodUnidadItem());
			        String desUnidad=parametrosSer.getTiposUnidades().stream().filter(tiposUnidades -> tiposUnidades.getCodUnidad().equals(det.getCodUnidadItem())).findFirst().map(TiposUnidades::getDesUnidad).orElse("-");
			        String[] desUnidadArray= desUnidad.split("\\(");
			        String desUnidadPrimerResultado = desUnidadArray[0].trim();
			        info.setDesUnidadMedida(desUnidadPrimerResultado);
			        info.setDesCodigo(det.getCodDetalleItem());
			        info.setDesItem(det.getDesDetalleItem());
			        info.setMtoDesc(obtenerMontoRubroxitem(rubrosRel, RubrosEnum.RUBRO_112, det.getDetalleComprobantePK().getNumFilaItem()));
			        info.setMtoICBPER(obtenerMontoRubroxitem(rubrosRel, RubrosEnum.RUBRO_985, det.getDetalleComprobantePK().getNumFilaItem()));
			        informacionItems.add(info);
			    
                }else{
	    						if (item.isPresent()) {
	    							info.setCntItems(det.getNumCantItem());
	    							info.setMtoValUnitario(item.get().getMtoValUnitario());
	    							info.setMtoImpTotal(item.get().getMtoImpTotal());
	    						} else {
	    							info.setCntItems(det.getNumCantItem());
	    							info.setMtoValUnitario(
	    									obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_111, det.getDetalleComprobantePK().getNumFilaItem()));
	    							if (obtenerDescripcionRubroxitem(rubros, RubrosEnum.RUBRO_224,
	    									det.getDetalleComprobantePK().getNumFilaItem()).equals(Constantes.INDICADOR_ANTICIPO)) {
	    								info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_233,
	    										det.getDetalleComprobantePK().getNumFilaItem()));
	    							} else {
	    								if ((obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
	    										det.getDetalleComprobantePK().getNumFilaItem())).compareTo(BigDecimal.ZERO) > 0) {
	    									info.setMtoImpTotal(obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
	    											det.getDetalleComprobantePK().getNumFilaItem()));
	    								} else {
	    									info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_134,
	    											det.getDetalleComprobantePK().getNumFilaItem()));
	    								}
	    							}
	    						}
	                info.setNumFila(det.getDetalleComprobantePK().getNumFilaItem());
	                
	                info.setCodUnidadMedida(det.getCodUnidadItem());
	                String desUnidad=parametrosSer.getTiposUnidades().stream().filter(tiposUnidades -> tiposUnidades.getCodUnidad().equals(det.getCodUnidadItem())).findFirst().map(TiposUnidades::getDesUnidad).orElse("-");
	                String[] desUnidadArray= desUnidad.split("\\(");
	                String desUnidadPrimerResultado = desUnidadArray[0].trim();
	                info.setDesUnidadMedida(desUnidadPrimerResultado);
	                info.setDesCodigo(det.getCodDetalleItem());
	                info.setDesItem(det.getDesDetalleItem());
	                info.setMtoDesc(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_112, det.getDetalleComprobantePK().getNumFilaItem()));
	                info.setMtoICBPER(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_985, det.getDetalleComprobantePK().getNumFilaItem()));
	                informacionItems.add(info);
	            }
            }
            comprobanteIndivual.setInformacionItems(informacionItems);
        }
        
        if(Arrays.asList(COD_PROCEDENCIAS_GEM).contains(comprobante.getIndProcedencia()) && 
        		 Arrays.asList(COD_NOTA_DEBITO_GEM).contains(comprobante.getCodTipoCpe())  && 
        		 Constantes.COD_CPE_NOTA_DEBITO.equals(comprobante.getComprobantePK().getCodCpe())){
        		 String codTipoCpeEquivalente;
        		        switch (comprobante.getCodTipoCpe()) {
        		            case "01":
        		            	codTipoCpeEquivalente = "91";
        		                break;
        		            case "02":
        		            	codTipoCpeEquivalente = "92";
        		                break;
        		            case "03":
        		            	codTipoCpeEquivalente = "93";
        		                break;
        		            case "11":
        		            	codTipoCpeEquivalente = "94";
        		                break;
        		            case "12":
        		            	codTipoCpeEquivalente = "95";
        		                break;
        		            default:
        		            	codTipoCpeEquivalente = "";
        		        }
        	comprobanteIndivual.setDesTipoNota(parametrosSer.getTiposNota().stream().filter(tiposNota -> tiposNota.getCodTipNota().equals(codTipoCpeEquivalente)).findFirst().map(TiposNota::getDesNota).orElse("-"));
        }else{
        	comprobanteIndivual.setDesTipoNota(parametrosSer.getTiposNota().stream().filter(tiposNota -> tiposNota.getCodTipNota().equals(comprobante.getCodTipoCpe())).findFirst().map(TiposNota::getDesNota).orElse("-"));
        }
        DatoRelacionado datoRelacionado = new DatoRelacionado();
        datoRelacionado.setCodTipNota(comprobante.getCodTipoCpe());
        if(Constantes.PROCEDENCIA_GEM.equals(comprobante.getIndProcedencia()) || "-".equals(comprobante.getDesObservacion()) || comprobante.getDesObservacion().isEmpty() || comprobante.getDesObservacion()==null) {
                        datoRelacionado.setDesMotivo(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_193));
                }else {
                        datoRelacionado.setDesMotivo(comprobante.getDesObservacion());
                }
        
        List<DocumentosModifica> lstdocumentosModifica = new ArrayList<>();

        if (!docRelacionado.isEmpty()) {
            for (DocumentoRelacionado documentoRel : docRelacionado.stream().filter(documentoRelacionado -> documentoRelacionado.getDocumentoRelacionadoPK().getCodDocRel().equals(Constantes.COD_CPE_BOLETA) || documentoRelacionado.getDocumentoRelacionadoPK().getCodDocRel().equals(Constantes.COD_CPE_FACTURA)).collect(Collectors.toList())) {
                DocumentosModifica documentosModifica = new DocumentosModifica();
                documentosModifica.setCodCpeRelac(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
                documentosModifica.setNumCpeRelac(documentoRel.getDocumentoRelacionadoPK().getNumDocRel());
                documentosModifica.setNumSerieRelac(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel());
                lstdocumentosModifica.add(documentosModifica);

            }

        } else {
            DocumentosModifica documentosModifica = new DocumentosModifica();
            documentosModifica.setCodCpeRelac(comprobanteIndivual.getCodDocRel());
            documentosModifica.setNumCpeRelac(String.valueOf(comprobanteIndivual.getNumCpeRel()));
            documentosModifica.setNumSerieRelac(comprobanteIndivual.getNumSerieRel());
            lstdocumentosModifica.add(documentosModifica);
        }


        if (!lstdocumentosModifica.isEmpty()) {
            datoRelacionado.setDocumentosModificaList(lstdocumentosModifica);
            comprobanteIndivual.setDatoDodRelacionado(datoRelacionado);

        }

        
        if (comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_PORTAL) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_EMPRENDER) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_SUNAT) 
        		|| comprobante.getIndProcedencia().equals(Constantes.APP_PERSONAS)) {
        	isPortal=true;
            ProcedenciaIndividual procedenciaIndividual = new ProcedenciaIndividual();
            
            if("27".equals(comprobante.getCodTipoCpe()) || "23".equals(comprobante.getCodTipoCpe())){
            	procedenciaIndividual.setMtoSubTotal(new BigDecimal("0"));
            }else{
            	procedenciaIndividual.setMtoSubTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_101));
            }
            procedenciaIndividual.setMtoOpExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
            procedenciaIndividual.setMtoOpGravado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
            procedenciaIndividual.setMtoOpInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
            //procedenciaIndividual.setMtoAnticipos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_960));
            procedenciaIndividual.setMtoAnticipos(obtenerMontoAnticipoXMLPortal(document));
            procedenciaIndividual.setMtoDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
            procedenciaIndividual.setMtoISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
            
            if (comprobante.getMtoTotalVenta().max(BigDecimal.ZERO)!= BigDecimal.ZERO) {
            	procedenciaIndividual.setMtoValorVenta(comprobante.getMtoTotalVenta().max(BigDecimal.ZERO));
	       	}else{
	       		procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110).max(BigDecimal.ZERO));
	      	}

            if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
                procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952).max(BigDecimal.ZERO));
                procedenciaIndividual.setMtoIVAP(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953).max(BigDecimal.ZERO));
            }else{
            	if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104) != BigDecimal.ZERO) {
           		 procedenciaIndividual.setMtoIGV(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104).max(BigDecimal.ZERO));
           	 }else{
           		procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv().max(BigDecimal.ZERO));
           	 }
            }

            procedenciaIndividual.setMtoOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
            procedenciaIndividual.setMtoOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
            procedenciaIndividual.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
            procedenciaIndividual.setMtoICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
            procedenciaIndividual.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
            comprobanteIndivual.setProcedenciaIndividual(procedenciaIndividual);
        }
        if(comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_GEM) 
        		||comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_OSE) 
        		||comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_BAJA_O_NULA_CONECTIVIDAD) ){


            ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();
            procedenciaMasiva.setMtoDtoGlobalAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_958));
            procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
            procedenciaMasiva.setMtoTotalValVentaInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
            procedenciaMasiva.setMtoTotalValVentaExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
            procedenciaMasiva.setMtoTotalValVentaGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_137));
            procedenciaMasiva.setMtoTotalValVentaExportacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_142));
            procedenciaMasiva.setMtoDtoGlobalNoAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_959));
            procedenciaMasiva.setMtoTotalDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
            procedenciaMasiva.setMtoSumOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
//            procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
            procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoOtrosCargosXMLGEM(document));
            procedenciaMasiva.setMtoSumISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
            if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
                procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952));
                procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953));
            }else {
              procedenciaMasiva.setMtoSumIGV(comprobante.getMtoTotalIgv());
            }
            procedenciaMasiva.setMtoSumICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
            procedenciaMasiva.setMtoTotalAnticipo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
            procedenciaMasiva.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
            procedenciaMasiva.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
            comprobanteIndivual.setProcedenciaMasiva(procedenciaMasiva);
        }
        if(comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_CONTINGENCIA_GEM_PORTAL) ){
            ComprobanteXML comprobanteXml = null;
            comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(comprobante.getComprobantePK().getNumRuc(), String.valueOf(comprobante.getComprobantePK().getNumCpe()), comprobante.getComprobantePK().getCodCpe(), comprobante.getComprobantePK().getNumSerieCpe());
            if(Objects.nonNull(comprobanteXml)){
                ProcedenciaIndividual procedenciaIndividual = new ProcedenciaIndividual();
                
                if("27".equals(comprobante.getCodTipoCpe()) || "23".equals(comprobante.getCodTipoCpe())){
                	procedenciaIndividual.setMtoSubTotal(new BigDecimal("0"));
                }else{
                	procedenciaIndividual.setMtoSubTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_101));
                }
                
                procedenciaIndividual.setMtoOpExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
                procedenciaIndividual.setMtoOpGravado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
                procedenciaIndividual.setMtoOpInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
                //procedenciaIndividual.setMtoAnticipos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_960));
                procedenciaIndividual.setMtoAnticipos(obtenerMontoAnticipoXMLPortal(document));
                procedenciaIndividual.setMtoDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
                procedenciaIndividual.setMtoISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));

                if (comprobante.getMtoTotalVenta().max(BigDecimal.ZERO)!= BigDecimal.ZERO) {
                	procedenciaIndividual.setMtoValorVenta(comprobante.getMtoTotalVenta().max(BigDecimal.ZERO));
    	       	}else{
    	       		procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110).max(BigDecimal.ZERO));
    	      	}
                
                if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
                    procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952).max(BigDecimal.ZERO));
                    procedenciaIndividual.setMtoIVAP(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953).max(BigDecimal.ZERO));
                }else{
                	if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104) != BigDecimal.ZERO) {
               		 procedenciaIndividual.setMtoIGV(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104).max(BigDecimal.ZERO));
               	 }else{
               		procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv().max(BigDecimal.ZERO));
               	 }
                }

                procedenciaIndividual.setMtoOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
                procedenciaIndividual.setMtoOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
                procedenciaIndividual.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
                procedenciaIndividual.setMtoICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
                procedenciaIndividual.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
                comprobanteIndivual.setProcedenciaIndividual(procedenciaIndividual);
            }else {

                ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();
                comprobanteIndivual.setIndProcedencia(Constantes.PROCEDENCIA_GEM);//cuando es contingencia (4) pero es GEM, el indicador debe ser GEM(2)
                procedenciaMasiva.setMtoDtoGlobalAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_958));
                procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
                procedenciaMasiva.setMtoTotalValVentaInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
                procedenciaMasiva.setMtoTotalValVentaExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
                procedenciaMasiva.setMtoTotalValVentaGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_137));
                procedenciaMasiva.setMtoTotalValVentaExportacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_142));
                procedenciaMasiva.setMtoDtoGlobalNoAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_959));
                procedenciaMasiva.setMtoTotalDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
                procedenciaMasiva.setMtoSumOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
//                procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
                procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoOtrosCargosXMLGEM(document));
                procedenciaMasiva.setMtoSumISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));


                if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953) != BigDecimal.ZERO) {
                    procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952));
                    procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953));
                } else {
                  procedenciaMasiva.setMtoSumIGV(comprobante.getMtoTotalIgv());
                }
                procedenciaMasiva.setMtoSumICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
                procedenciaMasiva.setMtoTotalAnticipo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
                procedenciaMasiva.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
                procedenciaMasiva.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
                comprobanteIndivual.setProcedenciaMasiva(procedenciaMasiva);
            }

        }
        if(Constantes.COD_CPE_NOTA_CREDITO.equals(comprobanteIndivual.getCodCpe()) && Constantes.COD_CPE_FACTURA.equals(comprobanteIndivual.getCodDocRel())){
            InformacionCredito informacionCredito = new InformacionCredito();
            informacionCredito.setMtoPagoPendiente(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_987));
            informacionCredito.setFecPlazoPago(obteneFechaRubro(rubros, RubrosEnum.RUBRO_995));
            List<InformacionCredito> informacionCreditoList = new ArrayList<>();

            informacionCredito.setNumCuotasList(obtenerCuotas(rubros));
            informacionCredito.setNumCuotas(informacionCredito.getNumCuotasList().size());
            informacionCreditoList.add(informacionCredito);
            comprobanteIndivual.setInformacionCreditos(informacionCreditoList);
        }
        
        List<InformacionDocumentosRelacionados> infoDocsRel = obtenerDocsRel(document);
        List<InformacionDocumentosRelacionados> informacionDocumentosRelacionados = new ArrayList<>();
        for (DocumentoRelacionado documentoRel : docRelacionado) {
        	String codAgrup = Arrays.asList(LIST_AGRUP_REL_NOTAS_CATALOGO1)
        			.contains(documentoRel.getCodRelacion()) ? Constantes.COD_AGRUP_REL_CATALOGO1 : Constantes.COD_AGRUP_REL_CATALOGO12;
        	boolean noEsRepetido=true;
        	for(DocumentosModifica docModificaQuitar : lstdocumentosModifica){
        		if(docModificaQuitar.getCodCpeRelac().equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel()) && 
        		   docModificaQuitar.getNumSerieRelac().equals(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel()) &&
        		   docModificaQuitar.getNumCpeRelac().equals(documentoRel.getDocumentoRelacionadoPK().getNumDocRel())
        		   ){
        			noEsRepetido=false;
        		}
        	}
            if (parametrosSer.getTiposComprobanteRelacionado().stream().anyMatch(tiposComprobanteRelacionado -> 
               tiposComprobanteRelacionado.getCodCpeRel().equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel()) 
               && tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup)) && noEsRepetido) {
            	                
                InformacionDocumentosRelacionados infdoc = new InformacionDocumentosRelacionados();
				
				Optional<InformacionDocumentosRelacionados> iDoc = infoDocsRel.stream()
						.filter(iDocRel -> (iDocRel.getNumSerieDocRel().equals("-")? true:iDocRel.getNumSerieDocRel().trim().equals(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel().trim()))
						&& iDocRel.getNumDocRel().trim().equals(documentoRel.getDocumentoRelacionadoPK().getNumDocRel()))
						.findFirst();
				if(isPortal && iDoc.isPresent() && iDoc.get().getDesCpeRel()!=null) {
					infdoc.setCodCpeRel(iDoc.get().getCodCpeRel());
					infdoc.setNumDocRel(iDoc.get().getNumDocRel());
					infdoc.setNumSerieDocRel(iDoc.get().getNumSerieDocRel());
					
					if(iDoc.get().getDesCpeRel().isEmpty() || iDoc.get().getDesCpeRel()==null){
						infdoc.setDesCpeRel(parametrosSer.getTiposComprobanteRelacionado().stream()
								.filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
										.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
										&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))
								.findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
					}else{
						infdoc.setDesCpeRel(iDoc.get().getDesCpeRel());
					}

					infdoc.setCodAgrupProc(codAgrup);
				} else {
					infdoc.setCodCpeRel(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
					infdoc.setNumDocRel(documentoRel.getDocumentoRelacionadoPK().getNumDocRel());
					infdoc.setNumSerieDocRel(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel());
					infdoc.setDesCpeRel(parametrosSer.getTiposComprobanteRelacionado().stream()
							.filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
									.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
									&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))
							.findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
					infdoc.setCodAgrupProc(codAgrup);
				}
				
				informacionDocumentosRelacionados.add(infdoc);

            }

        }
        if (!informacionDocumentosRelacionados.isEmpty()) {
            comprobanteIndivual.setInformacionDocumentosRelacionados(informacionDocumentosRelacionados);

        }

        
        return  comprobanteIndivual;
    }

    private Comprobantes llenarDatosLiquidacion(Comprobante comprobante,List<DetalleComprobante> detalle,List<Rubros> rubros,Document document,ParametrosSer parametrosSer,Date fecEmision,List<DocumentoRelacionado> docRelacionado) throws IOException, ParserConfigurationException, SAXException, TransformerException {
        Comprobantes comprobanteIndivual= new Comprobantes();
        List<InformacionItems> informacionItems = new ArrayList<>();
        DatosEmisor datosEmisor = new DatosEmisor();
        DatosReceptor datosReceptor = new DatosReceptor();
        boolean isPortal=false;
        
        datosEmisor.setDesDirEmis(obtenerValorNodeDireccion(document,comprobante.getComprobantePK().getCodCpe()));
        String dirEstEmisor=obtenerValorNodeDirecEstEmisor(document);
        if(!dirEstEmisor.isEmpty()){
            datosEmisor.setDesDirEstEmis(dirEstEmisor.trim());

        }
        datosEmisor.setUbigeoEmis(obtenerValorUbigeo(document,comprobante.getComprobantePK().getCodCpe()));
        datosEmisor.setNumRuc(comprobante.getComprobantePK().getNumRuc());
        datosEmisor.setDesRazonSocialEmis(obtenerValorNodeRazonSocial(document,comprobante.getComprobantePK().getCodCpe()));
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "datosEmisor.getDesRazonSocialEmis===="+datosEmisor.getDesRazonSocialEmis());
        datosEmisor.setDesNomComercialEmis(obtenerValorNodeRazonComercial(document,comprobante.getComprobantePK().getCodCpe()));

        datosReceptor.setCodDocIdeRecep(comprobante.getCodDocIdeRecep());
        datosReceptor.setNumDocIdeRecep(comprobante.getNumDocIdeRecep());
        datosReceptor.setDesRazonSocialRecep(obtenerValorNodeRazonSocialReceptor(document,comprobante.getComprobantePK().getCodCpe()));
        utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "datosReceptor.getDesRazonSocialEmis===="+datosReceptor.getDesRazonSocialRecep());
        
        datosReceptor.setDirDetCliente(obtenerValorNodeDirecReceptor(document,comprobante.getComprobantePK().getCodCpe()));
        datosReceptor.setDirDetVendedor(obtenerDatosDireccion(rubros, comprobante.getComprobantePK(), 21));
        datosReceptor.setDersLugarOperacion(obtenerDatosDireccion(rubros, comprobante.getComprobantePK(), 22));
        comprobanteIndivual.setDatosEmisor(datosEmisor);
        comprobanteIndivual.setDatosReceptor(datosReceptor);

        comprobanteIndivual.setNumSerie(comprobante.getComprobantePK().getNumSerieCpe());
        comprobanteIndivual.setCodCpe(comprobante.getComprobantePK().getCodCpe());

        comprobanteIndivual.setNumCpe(comprobante.getComprobantePK().getNumCpe());
        comprobanteIndivual.setCodMoneda(comprobante.getCodMoneda());
        if (!comprobante.getCodDocIdeRecep().isEmpty() && !comprobante.getCodDocIdeRecep().equals("-")) {
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(comprobante.getCodDocIdeRecep().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

        } else {
        		comprobanteIndivual.setDesTipoCpe("-");
        }
        if("00".endsWith(comprobante.getCodDocIdeRecep().trim()) || "0".endsWith(comprobante.getCodDocIdeRecep().trim())){
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
    	}
        comprobanteIndivual.setDesMoneda(parametrosSer.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(comprobante.getCodMoneda())).findFirst().map(TiposMoneda::getDesMoneda).orElse("-"));
        comprobanteIndivual.setDesSimbolo(parametrosSer.getTiposMoneda().stream().filter(tiposMoneda -> tiposMoneda.getCodMoneda().equals(comprobante.getCodMoneda())).findFirst().map(TiposMoneda::getCodSimbolo).orElse("-"));
        comprobanteIndivual.setFecEmision(comprobante.getFecEmision());
        comprobanteIndivual.setFecRegistro(comprobante.getFecEmision());
        comprobanteIndivual.setFecVencimiento(obteneFechaRubro(rubros, RubrosEnum.RUBRO_158));

        comprobanteIndivual.setCodTipTransaccion(obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_986));
        comprobanteIndivual.setIndEstadoCpe(comprobante.getIndEstado());
        comprobanteIndivual.setIndProcedencia(comprobante.getIndProcedencia());
        
        comprobanteIndivual.setIndTituloGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_131).compareTo(BigDecimal.ZERO) > 0 ? "1" : "0");
        
        comprobanteIndivual.setMtoVentaOpGratuita(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_131));
        comprobanteIndivual.setDesMtoTotalLetras(Numero.numerosALetras(comprobante.getMtoImporteTotal()) + " "
                + ComprobanteUtilBean.getDescripcionMoneda(comprobante.getCodMoneda()));
        comprobanteIndivual.setDesObservacion(comprobante.getDesObservacion());
        
        
        List<InformacionItems> listItems = obtenerInformacionItems(document);
        for (DetalleComprobante det : detalle) {
            InformacionItems info = new InformacionItems();
            Optional<InformacionItems> item = listItems.stream().filter(infoItem -> 
            det.getDetalleComprobantePK().getNumFilaItem() == infoItem.getNumFila()).findFirst();
            
						if (item.isPresent()) {
							info.setCntItems(item.get().getCntItems());
							info.setMtoValUnitario(item.get().getMtoValUnitario());
							info.setMtoImpTotal(item.get().getMtoImpTotal());
						} else {
							info.setCntItems(det.getNumCantItem());
							info.setMtoValUnitario(
									obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_111, det.getDetalleComprobantePK().getNumFilaItem()));
							if (obtenerDescripcionRubroxitem(rubros, RubrosEnum.RUBRO_224,
									det.getDetalleComprobantePK().getNumFilaItem()).equals(Constantes.INDICADOR_ANTICIPO)) {
								info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_233,
										det.getDetalleComprobantePK().getNumFilaItem()));
							} else {
								if ((obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
										det.getDetalleComprobantePK().getNumFilaItem())).compareTo(BigDecimal.ZERO) > 0) {
									info.setMtoImpTotal(obtenerMontoDescRubroxItem(rubros, RubrosEnum.RUBRO_119,
											det.getDetalleComprobantePK().getNumFilaItem()));
								} else {
									info.setMtoImpTotal(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_134,
											det.getDetalleComprobantePK().getNumFilaItem()));
								}
							}
						}
            info.setNumFila(det.getDetalleComprobantePK().getNumFilaItem());
            
            info.setCodUnidadMedida(det.getCodUnidadItem());
            String desUnidad=parametrosSer.getTiposUnidades().stream().filter(tiposUnidades -> tiposUnidades.getCodUnidad().equals(det.getCodUnidadItem())).findFirst().map(TiposUnidades::getDesUnidad).orElse("-");
            String[] desUnidadArray= desUnidad.split("\\(");
            String desUnidadPrimerResultado = desUnidadArray[0].trim();
            info.setDesUnidadMedida(desUnidadPrimerResultado);
            info.setDesCodigo(obtenerDescripcionRubroxitem(rubros, RubrosEnum.RUBRO_310, det.getDetalleComprobantePK().getNumFilaItem()));
            info.setDesItem(det.getDesDetalleItem());
            info.setMtoDesc(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_112, det.getDetalleComprobantePK().getNumFilaItem()));
            info.setMtoICBPER(obtenerMontoRubroxitem(rubros, RubrosEnum.RUBRO_985, det.getDetalleComprobantePK().getNumFilaItem()));
            informacionItems.add(info);
        }
        comprobanteIndivual.setInformacionItems(informacionItems);

        if (comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_PORTAL) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_EMPRENDER) 
        		|| comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_APP_SUNAT) 
        		|| comprobante.getIndProcedencia().equals(Constantes.APP_PERSONAS)) {
        	isPortal=true;
            ProcedenciaIndividual procedenciaIndividual = new ProcedenciaIndividual();
            procedenciaIndividual.setMtoSubTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_101));
            procedenciaIndividual.setMtoOpExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
            procedenciaIndividual.setMtoOpGravado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
            procedenciaIndividual.setMtoOpInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
            procedenciaIndividual.setMtoAnticipos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
            //procedenciaIndividual.setMtoAnticipos(obtenerMontoAnticipoXMLPortal(document));
            procedenciaIndividual.setMtoDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
            procedenciaIndividual.setMtoISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
            
            if (comprobante.getMtoTotalVenta().max(BigDecimal.ZERO)!= BigDecimal.ZERO) {
            	procedenciaIndividual.setMtoValorVenta(comprobante.getMtoTotalVenta().max(BigDecimal.ZERO));
	       	}else{
	       		procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110).max(BigDecimal.ZERO));
	      	}

            if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
                procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952).max(BigDecimal.ZERO));
                procedenciaIndividual.setMtoIVAP(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953).max(BigDecimal.ZERO));
            }else{
            	if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104) != BigDecimal.ZERO) {
           		 procedenciaIndividual.setMtoIGV(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104).max(BigDecimal.ZERO));
           	 }else{
           		 //procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv());
           		procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv().max(BigDecimal.ZERO));
           	 }
            }

            procedenciaIndividual.setMtoOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
            procedenciaIndividual.setMtoOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
            procedenciaIndividual.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
            procedenciaIndividual.setMtoICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
            procedenciaIndividual.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
            procedenciaIndividual.setMtoIgvCredito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_171));
            procedenciaIndividual.setMtoIrRetencion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_313));
            procedenciaIndividual.setMtoTotalLiquidacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_170));
            comprobanteIndivual.setProcedenciaIndividual(procedenciaIndividual);
        }
        if(comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_GEM) ||comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_OSE)||comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_BAJA_O_NULA_CONECTIVIDAD) ){

        

            ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();
            procedenciaMasiva.setMtoDtoGlobalAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_958));
            
            if (comprobante.getMtoTotalVenta()!= BigDecimal.ZERO) {
            	procedenciaMasiva.setMtoTotalValVentaGrabado(comprobante.getMtoTotalVenta());
	       	}else{
	       		procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
	      	}
   
            procedenciaMasiva.setMtoTotalValVentaInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
            procedenciaMasiva.setMtoTotalValVentaExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
            procedenciaMasiva.setMtoTotalValVentaGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_137));
            procedenciaMasiva.setMtoTotalValVentaExportacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_142));
            procedenciaMasiva.setMtoDtoGlobalNoAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_959));
            procedenciaMasiva.setMtoTotalDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
            procedenciaMasiva.setMtoSumOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
//            procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
            procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoOtrosCargosXMLGEM(document));
            procedenciaMasiva.setMtoSumISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
            if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
                procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952));
                procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953));
            }else {
                procedenciaMasiva.setMtoSumIGV(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_114));
            }
            procedenciaMasiva.setMtoSumICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
            procedenciaMasiva.setMtoTotalAnticipo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
            procedenciaMasiva.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
            procedenciaMasiva.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
            
            
            if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
            	procedenciaMasiva.setMtoSumIgvCredito(procedenciaMasiva.getMtoSumIVAP().add(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105)));
	       	}else{
	       		procedenciaMasiva.setMtoSumIgvCredito(procedenciaMasiva.getMtoSumIGV().add(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105)));
	      	}
            
            
            procedenciaMasiva.setMtoSumIrRetencion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_313));
            procedenciaMasiva.setMtoTotalValVentaLiquidacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_956));
            
            if (comprobante.getMtoTotalVenta()!= BigDecimal.ZERO) {
            	procedenciaMasiva.setMtoTotalValVenta(comprobante.getMtoTotalVenta());
	       	}else{
	       		procedenciaMasiva.setMtoTotalValVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
	      	}
            
            comprobanteIndivual.setProcedenciaMasiva(procedenciaMasiva);
        }
        if(comprobante.getIndProcedencia().equals(Constantes.PROCEDENCIA_CONTINGENCIA_GEM_PORTAL) ){
            ComprobanteXML comprobanteXml = null;
            comprobanteXml = comprobanteXMLRepository.findComprobanteXmlService(comprobante.getComprobantePK().getNumRuc(), String.valueOf(comprobante.getComprobantePK().getNumCpe()), comprobante.getComprobantePK().getCodCpe(), comprobante.getComprobantePK().getNumSerieCpe());
            if(Objects.nonNull(comprobanteXml)){
                ProcedenciaIndividual procedenciaIndividual = new ProcedenciaIndividual();
                procedenciaIndividual.setMtoSubTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_101));
                procedenciaIndividual.setMtoOpExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
                procedenciaIndividual.setMtoOpGravado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
                procedenciaIndividual.setMtoOpInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
                procedenciaIndividual.setMtoAnticipos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
                //procedenciaIndividual.setMtoAnticipos(obtenerMontoAnticipoXMLPortal(document));
                procedenciaIndividual.setMtoDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
                procedenciaIndividual.setMtoISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));
                
                if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_130)!= BigDecimal.ZERO) {
                	procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_130).max(BigDecimal.ZERO));
    	       	}else{
    	       		procedenciaIndividual.setMtoValorVenta(comprobante.getMtoTotalVenta().max(BigDecimal.ZERO));
    	      	}
                
                if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
                    procedenciaIndividual.setMtoValorVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952).max(BigDecimal.ZERO));
                    procedenciaIndividual.setMtoIVAP(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953).max(BigDecimal.ZERO));
                }else{
                	if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104) != BigDecimal.ZERO) {
               		 procedenciaIndividual.setMtoIGV(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_104).max(BigDecimal.ZERO));
               	 }else{
               		procedenciaIndividual.setMtoIGV(comprobante.getMtoTotalIgv().max(BigDecimal.ZERO).max(BigDecimal.ZERO));
               	 }
                }

                procedenciaIndividual.setMtoOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
                procedenciaIndividual.setMtoOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
                procedenciaIndividual.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
                procedenciaIndividual.setMtoICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
                procedenciaIndividual.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
                procedenciaIndividual.setMtoIgvCredito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_171));
                procedenciaIndividual.setMtoIrRetencion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_313));
                procedenciaIndividual.setMtoTotalLiquidacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_170));
                comprobanteIndivual.setProcedenciaIndividual(procedenciaIndividual);
            }else {

                ProcedenciaMasiva procedenciaMasiva = new ProcedenciaMasiva();
                comprobanteIndivual.setIndProcedencia(Constantes.PROCEDENCIA_GEM);//cuando es contingencia (4) pero es GEM, el indicador debe ser GEM(2)
                procedenciaMasiva.setMtoDtoGlobalAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_958));
                
                if (comprobante.getMtoTotalVenta()!= BigDecimal.ZERO) {
                	procedenciaMasiva.setMtoTotalValVentaGrabado(comprobante.getMtoTotalVenta());
    	       	}else{
    	       		procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
    	      	}
                
                procedenciaMasiva.setMtoTotalValVentaInafecto(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_108));
                procedenciaMasiva.setMtoTotalValVentaExonerado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_109));
                procedenciaMasiva.setMtoTotalValVentaGratuito(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_137));
                procedenciaMasiva.setMtoTotalValVentaExportacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_142));
                procedenciaMasiva.setMtoDtoGlobalNoAfecBI(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_959));
                procedenciaMasiva.setMtoTotalDtos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_102));
                procedenciaMasiva.setMtoSumOtrosTributos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105));
//                procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_106));
                procedenciaMasiva.setMtoSumOtrosCargos(obtenerMontoOtrosCargosXMLGEM(document));
                procedenciaMasiva.setMtoSumISC(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_103));


                if (obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953) != BigDecimal.ZERO) {
                    procedenciaMasiva.setMtoTotalValVentaGrabado(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_952));
                    procedenciaMasiva.setMtoSumIVAP(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_953));
                } else {
                    procedenciaMasiva.setMtoSumIGV(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_114));
                }
                procedenciaMasiva.setMtoSumICBPER(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_984));
                procedenciaMasiva.setMtoTotalAnticipo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_166));
                procedenciaMasiva.setMtoImporteTotal(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_107));
                procedenciaMasiva.setMtoRedondeo(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_957));
                
                if (comprobante.getMtoTotalVenta()!= BigDecimal.ZERO) {
                	procedenciaMasiva.setMtoTotalValVenta(comprobante.getMtoTotalVenta());
    	       	}else{
    	       		procedenciaMasiva.setMtoTotalValVenta(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_110));
    	      	}
                
                if(obtenerMontoRubro(rubros,RubrosEnum.RUBRO_953) != BigDecimal.ZERO){
                	procedenciaMasiva.setMtoSumIgvCredito(procedenciaMasiva.getMtoSumIVAP().add(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105)));
    	       	}else{
    	       		procedenciaMasiva.setMtoSumIgvCredito(procedenciaMasiva.getMtoSumIGV().add(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_105)));
    	      	}
                
                procedenciaMasiva.setMtoSumIrRetencion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_313));
                procedenciaMasiva.setMtoTotalValVentaLiquidacion(obtenerMontoRubro(rubros, RubrosEnum.RUBRO_956));
                comprobanteIndivual.setProcedenciaMasiva(procedenciaMasiva);
            }


        }
        List<VehiculoTransporte> vehiculosTransporte = new ArrayList<>();
        VehiculoPK vehiculopk = new VehiculoPK();
        vehiculopk.setNumSerieCpe(comprobante.getComprobantePK().getNumSerieCpe());
        vehiculopk.setCodCpe(comprobante.getComprobantePK().getCodCpe());
        vehiculopk.setNumRuc(comprobante.getComprobantePK().getNumRuc());
        vehiculopk.setNumCpe(comprobante.getComprobantePK().getNumCpe());
        List<Vehiculo> vehiculos = Optional
                .ofNullable(vehiculoRepository.listarVehiculos(vehiculopk)).orElse(null);
        if (Objects.nonNull(vehiculos)) {
            for (Vehiculo vehiculo : vehiculos) {
                VehiculoTransporte vehiculoTransporte = new VehiculoTransporte();
                vehiculoTransporte.setPlaca(vehiculo.numPlaca);
                vehiculosTransporte.add(vehiculoTransporte);
            }
            if (!vehiculosTransporte.isEmpty()) {
                comprobanteIndivual.setVehiculosTransporte(vehiculosTransporte);

            }
        }
                
		
		List<InformacionDocumentosRelacionados> infoDocsRel = obtenerDocsRel(document);
		List<InformacionDocumentosRelacionados> informacionDocumentosRelacionados = new ArrayList<>();
		List<String> docRelAnticipo = Arrays.asList("01","02","03");
		 
		for (DocumentoRelacionado documentoRel : docRelacionado) {
			String codAgrup = Arrays.asList(LIST_AGRUP_REL_FACBOL_CATALOGO1)
					.contains(documentoRel.getCodRelacion())
							? Constantes.COD_AGRUP_REL_CATALOGO1
							: Arrays.asList(LIST_AGRUP_REL_FACBOL_CATALOGO12).contains(documentoRel.getCodRelacion())
									? Constantes.COD_AGRUP_REL_CATALOGO12 : "-";
			boolean isExistsDocRel = docRelAnticipo.contains(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
			if (parametrosSer.getTiposComprobanteRelacionado().stream()
					.anyMatch(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
							.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
							&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup)&& !isExistsDocRel)) {
				
				InformacionDocumentosRelacionados infdoc = new InformacionDocumentosRelacionados();
				
				Optional<InformacionDocumentosRelacionados> iDoc = infoDocsRel.stream()
						.filter(iDocRel -> (iDocRel.getNumSerieDocRel().equals("-")? true:iDocRel.getNumSerieDocRel().trim().equals(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel().trim()))
						&& iDocRel.getNumDocRel().trim().equals(documentoRel.getDocumentoRelacionadoPK().getNumDocRel()))
						.findFirst();
				if(isPortal && iDoc.isPresent() && iDoc.get().getDesCpeRel()!=null) {
					infdoc.setCodCpeRel(iDoc.get().getCodCpeRel());
					infdoc.setNumDocRel(iDoc.get().getNumDocRel());
					infdoc.setNumSerieDocRel(iDoc.get().getNumSerieDocRel());
					
					if(iDoc.get().getDesCpeRel().isEmpty() || iDoc.get().getDesCpeRel()==null){
						infdoc.setDesCpeRel(parametrosSer.getTiposComprobanteRelacionado().stream()
								.filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
										.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
										&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))
								.findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
					}else{
						infdoc.setDesCpeRel(iDoc.get().getDesCpeRel());
					}
					
					infdoc.setCodAgrupProc(codAgrup);
				} else {
					infdoc.setCodCpeRel(documentoRel.getDocumentoRelacionadoPK().getCodDocRel());
					infdoc.setNumDocRel(documentoRel.getDocumentoRelacionadoPK().getNumDocRel());
					infdoc.setNumSerieDocRel(documentoRel.getDocumentoRelacionadoPK().getNumSerieDocRel());
					infdoc.setDesCpeRel(parametrosSer.getTiposComprobanteRelacionado().stream()
							.filter(tiposComprobanteRelacionado -> tiposComprobanteRelacionado.getCodCpeRel()
									.equals(documentoRel.getDocumentoRelacionadoPK().getCodDocRel())
									&& tiposComprobanteRelacionado.getCodAgrupadoProcedencia().equals(codAgrup))
							.findFirst().map(TiposComprobanteRelacionado::getDesCpeRel).orElse("-"));
					infdoc.setCodAgrupProc(codAgrup);
				}
				
				informacionDocumentosRelacionados.add(infdoc);

			}

		}
		
		
		if (!informacionDocumentosRelacionados.isEmpty()) {
            comprobanteIndivual.setInformacionDocumentosRelacionados(informacionDocumentosRelacionados);
        }
     
        return  comprobanteIndivual;
    }

    public String obtenerDatosDireccion(List<Rubros> rubros, ComprobantePK comprobantePk, int tipoDireccion) {
        String dirDatos = "";
        DireccionPK direccionPK = new DireccionPK();
        direccionPK.setCodCpe(comprobantePk.getCodCpe());
        direccionPK.setNumRuc(comprobantePk.getNumRuc());
        direccionPK.setNumSerieCpe(comprobantePk.getNumSerieCpe());
        direccionPK.setNumCpe(comprobantePk.getNumCpe());
        direccionPK.setCodTipoDireccion(tipoDireccion);
        Direccion direccion = Optional
                .ofNullable(direccionRepository.obtenerDireccion(direccionPK))
                .orElse(null);
        if (Objects.nonNull(direccion)) {
            if (!direccion.getCodUbigeo().isEmpty()) {
                String tipoUbigeo = "031";

                String departamento = ubigeoRepository.obtenerDepartamentos(tipoUbigeo, direccion.getCodUbigeo().substring(0, 2).trim() + "0000").stream().findFirst().map(UbigeoVO::getDesParam).orElse("-");
                String provincia = ubigeoRepository.obtenerProvinvias(tipoUbigeo, direccion.getCodUbigeo().substring(0, 4).trim() + "00").stream().findFirst().map(UbigeoVO::getDesParam).orElse("-");
                String distrito = ubigeoRepository.obtenerDistritos(tipoUbigeo, direccion.getCodUbigeo().substring(0, 2).trim() + "0000", direccion.getCodUbigeo().substring(0, 4).trim() + "00", direccion.getCodUbigeo().trim()).stream().findFirst().map(UbigeoVO::getDesParam).orElse("-");

                dirDatos = direccion.getDesDireccion() + " " + departamento + "-" + provincia + "-" + distrito;
            } else {
                dirDatos = direccion.getDesDireccion();
            }


        } else {
            if (tipoDireccion == 21) {
                dirDatos = obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_989);

            }
            if (tipoDireccion == 22) {
                dirDatos = obtenerDescripcionRubro(rubros, RubrosEnum.RUBRO_988);
            }

        }
        return dirDatos;
    }

    public String obtenerDescripcionRubro(List<Rubros> rubros, RubrosEnum codigo) {
        String cod = codigo.getCode();
        try {
        	Optional<String> descripcion = rubros.stream().filter(e -> e.getRubrosPK().getCodRubro().equals(cod))
          		.findFirst().map(Rubros::getDesDetalleRubro);
        	return descripcion.orElse("");
        }catch(Exception e) {
        }
        return "";
        
    }


    public BigDecimal obtenerMontoRubroxitem(List<Rubros> rubros, RubrosEnum codigo, int item) {
        String cod = codigo.getCode();
        Optional<BigDecimal> monto = rubros.stream().filter(e -> e.getRubrosPK().getCodRubro().equals(cod) && e.getRubrosPK().getNumFilaItem() == item).findFirst().map(Rubros::getMtoRubro);
        return monto.orElse(BigDecimal.ZERO);
    }
    
    public BigDecimal obtenerMontoDescRubroxItem(List<Rubros> rubros, RubrosEnum codigo, int item) {
      String cod = codigo.getCode();
      
      Optional<Rubros> rubro = rubros.stream().filter(e -> e.getRubrosPK().getCodRubro().equals(cod) && e.getRubrosPK().getNumFilaItem() == item).findFirst();
      if (rubro.isPresent()) {
      	if(!rubro.get().getDesDetalleRubro().trim().isEmpty()) {
      		try{
      			return BigDecimal.valueOf(Double.parseDouble(rubro.get().getDesDetalleRubro().trim().replaceAll(",", "")));
      		}catch(NumberFormatException e){
      			return rubro.get().getMtoRubro();
      		}
      		
      	}else {
      		return rubro.get().getMtoRubro();
      	}
      }
      return BigDecimal.ZERO;
  }

    public String obtenerDescripcionRubroxitem(List<Rubros> rubros, RubrosEnum codigo, int item) {
        String cod = codigo.getCode();
        Optional<String> descripcion = rubros.stream().filter(e -> e.getRubrosPK().getCodRubro().equals(cod) && e.getRubrosPK().getNumFilaItem() == item).findFirst().map(Rubros::getDesDetalleRubro);
        return descripcion.orElse("");
    }

        public BigDecimal obtenerMontoRubro(List<Rubros> rubros, RubrosEnum codigo) {
            String cod = codigo.getCode();
            
            if (cod.equals("9999")) {
                String codpecep = "941";
                String codVenta = "956";
                
                BigDecimal monto = rubros.stream()
                        .filter(e -> e.getRubrosPK().getCodRubro().equals(codpecep))
                        .findFirst()
                        .map(Rubros::getMtoRubro)
                        .orElse(BigDecimal.ZERO);
                        
                BigDecimal montoVenta = rubros.stream()
                        .filter(e -> e.getRubrosPK().getCodRubro().equals(codVenta))
                        .findFirst()
                        .map(Rubros::getMtoRubro)
                        .orElse(BigDecimal.ZERO);
                        
                return monto.add(montoVenta);
            } else {
                BigDecimal monto = rubros.stream()
                        .filter(e -> Objects.nonNull(e.getRubrosPK()) && cod.equals(e.getRubrosPK().getCodRubro()))
                        .map(Rubros::getMtoRubro)
                        .filter(Objects::nonNull) // Asegúrate de que getMtoRubro() no devuelva null
                        .reduce(BigDecimal.ZERO, BigDecimal::add); // Reduce con un valor inicial

                return monto;
            }
        }
  

    public Date obteneFechaRubro(List<Rubros> rubros, RubrosEnum codigo) {
        String cod = codigo.getCode();
        Optional<Date> fecha = rubros.stream().filter(e -> e.getRubrosPK().getCodRubro().equals(cod)).findFirst().map(Rubros::getFecRubro);
        if (fecha.isPresent()) {
            return fecha.get();

        } else {
            return null;
        }
    }

    public List<NumCuotas> obtenerCuotas(List<Rubros> rubros) {
        List<Rubros> rubroCuota;
        List<NumCuotas> numCuotas = new ArrayList<>();
        rubroCuota = rubros.stream().filter(e -> e.getIndCabDet().equals("4")).collect(Collectors.toList());
        Map<Integer, List<Rubros>> grupos = new HashMap<>();

        for (Rubros rubr : rubroCuota) {
        	if(rubr.getRubrosPK().getNumFilaItem()>0){
	            Integer numeroFila = rubr.getRubrosPK().getNumFilaItem();
	            if (!grupos.containsKey(numeroFila)) {
	                grupos.put(numeroFila, new ArrayList<>());
	            }
	            // Agregar el objeto al grupo correspondiente
	            grupos.get(numeroFila).add(rubr);
        	}
        }
        for (Integer campo : grupos.keySet()) {

            List<Rubros> grupo = grupos.get(campo);
            NumCuotas numCuotas1 = new NumCuotas();
            numCuotas1.setNumcuota(grupo.stream().filter(e -> e.getRubrosPK().getCodRubro().equals("993")).findFirst().map(Rubros::getRubrosPK).map(t -> t.getNumFilaItem()).orElse(0));
            numCuotas1.setMtoCuota(grupo.stream().filter(e -> e.getRubrosPK().getCodRubro().equals("994")).findFirst().map(Rubros::getMtoRubro).orElse(BigDecimal.ZERO));
            numCuotas1.setFecVencimiento(grupo.stream().filter(e -> e.getRubrosPK().getCodRubro().equals("995")).findFirst().map(Rubros::getFecRubro).orElse(null)); 
            numCuotas.add(numCuotas1);
            
        }
        return numCuotas;
    }


    private Document obtenerValorNodoXml(String numRuc, String codCpe, String serieCpe, String numCpe, String codCpeOri, String tipoConsulta) 
    		throws IOException, ParserConfigurationException, SAXException, TransformerException {
        return recuperaComprobanteXmlDoc(numRuc, numCpe, codCpe, serieCpe);
    }

    private String obtenerValorNodeRazonSocial(Document newDocument,String codCpe) throws TransformerException, ParserConfigurationException, SAXException, IOException {
    	
        String desRazonSocialEmis = "-";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            if(codCpe.equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)){
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingCustomerParty");

            }else{
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingSupplierParty");

            }
            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:Party");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:PartyLegalEntity");
                    NodeList partyLegalEntityList1 = partyElement.getElementsByTagName("cac:PartyTaxScheme");

                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);

                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cbc:RegistrationName");

                        if (registrationNameList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            Element registrationNameElement = (Element) registrationNameList.item(0);

                            // Obtener el texto contenido en el elemento cbc:RegistrationName
                            String registrationNameValue = registrationNameElement.getTextContent();

                            desRazonSocialEmis = registrationNameValue;
                        }
                    }else if(partyLegalEntityList1.getLength() > 0){

                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList1.item(0);

                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cbc:RegistrationName");

                        if (registrationNameList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            Element registrationNameElement = (Element) registrationNameList.item(0);

                            // Obtener el texto contenido en el elemento cbc:RegistrationName
                            String registrationNameValue = registrationNameElement.getTextContent();

                            desRazonSocialEmis = registrationNameValue;
                        }
                    
                    }
                }
            }
        }

        return desRazonSocialEmis.replaceAll("[\\n\\t]", "").trim();
    }

    private String obtenerValorNodeRazonComercial(Document newDocument,String codCpe) throws TransformerException, ParserConfigurationException, SAXException, IOException {
    	
        String desRazonComercialEmis = "-";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;

            if(codCpe.equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)){
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingCustomerParty");

            }else{
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingSupplierParty");

            }
            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:Party");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:PartyName");

                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);

                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cbc:Name");

                        if (registrationNameList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            Element registrationNameElement = (Element) registrationNameList.item(0);

                            // Obtener el texto contenido en el elemento cbc:RegistrationName
                            String registrationNameValue = registrationNameElement.getTextContent();

                            desRazonComercialEmis = registrationNameValue;
                        }
                    }
                }
            }
        }

        return desRazonComercialEmis.replaceAll("[\\n\\t]", "").trim();
    }

    private String obtenerValorNodeDireccion(Document newDocument,String codCpe) throws TransformerException, ParserConfigurationException, SAXException, IOException {
    	
        String direccion = "-";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            NodeList accountingSupplierPartyList1;
            if(codCpe.equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)){
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingCustomerParty");
                accountingSupplierPartyList1 = newDocument.getElementsByTagName("cac:DeliveryTerms");

            }else{
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingSupplierParty");
                accountingSupplierPartyList1 = newDocument.getElementsByTagName("cac:DeliveryTerms");
            }
            
            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:Party");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:PartyLegalEntity");
                    // Buscar el elemento cac:PostalAddress para Contingencia
                    NodeList postalAddressContin = partyElement.getElementsByTagName("cac:PostalAddress");
                    
                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);

                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationAddressList = partyLegalEntityElement.getElementsByTagName("cac:RegistrationAddress");

                        if (registrationAddressList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            NodeList addressLineList = partyLegalEntityElement.getElementsByTagName("cac:AddressLine");
                            if (addressLineList.getLength() > 0) {
                                Element addressLineElement = (Element) addressLineList.item(0);

                                direccion = addressLineElement.getTextContent();

                            }
                            // Obtener el texto contenido en el elemento cbc:RegistrationName

                        }else{
                            if (postalAddressContin.getLength() > 0) {
                                Element StreetNameDir = (Element) postalAddressContin.item(0);
                                NodeList StreetNameList = StreetNameDir.getElementsByTagName("cbc:StreetName");
                                if (StreetNameList.getLength() > 0) {
                                    // Obtener el primer elemento cbc:RegistrationName
                                    Element StreetElement = (Element) StreetNameList.item(0);
                                    // Obtener el texto contenido en el elemento cbc:StreetName
                                    direccion = StreetElement.getTextContent();
                                }
                            }
                        
                        }
                    }
                }
            }else if(accountingSupplierPartyList1.getLength() > 0){

                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList1.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:DeliveryLocation");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:Address");
                    
                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);
                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationAddressList = partyLegalEntityElement.getElementsByTagName("cbc:StreetName");
                        if (registrationAddressList.getLength() > 0) {
                                Element addressLineElement = (Element) registrationAddressList.item(0);
                                direccion = addressLineElement.getTextContent();
                        }
                    }
                }       	
        	} 
        } else {
            direccion = "-";
        }



        return  direccion.replaceAll("[\\n\\t]", "").trim();
    }
    
		private List<InformacionItems> obtenerInformacionItems(Document document) {
			
			List<InformacionItems> listItems = new ArrayList<InformacionItems>();
			InformacionItems items = null;
			
			if(document!=null) {
			NodeList nlItems = document.getElementsByTagName("cac:InvoiceLine");
			if (nlItems.getLength() > 0) {
				for (int i = 0; i < nlItems.getLength(); i++) {
					items = new InformacionItems();

					Element invoiceLine = (Element) nlItems.item(i);

					NodeList idList = invoiceLine.getElementsByTagName("cbc:ID");

					if (idList.getLength() > 0) {
						Element idElement = (Element) idList.item(0);
						String id = idElement.getTextContent();
						items.setNumFila(Integer.parseInt(id));
					}
					
					

					NodeList invoicedQuantity = invoiceLine.getElementsByTagName("cbc:InvoicedQuantity");
					if (invoicedQuantity.getLength() > 0) {
						Element idElement = (Element) invoicedQuantity.item(0);
						String cantidad = idElement.getTextContent();
						try {
							if (cantidad == null || "".equals(cantidad) || "0".equals(cantidad)) {
								items.setCntItems(new BigDecimal("1.00"));
							} else {
								items.setCntItems(new BigDecimal(cantidad));
							}
						} catch(Exception e) {
							items.setCntItems(new BigDecimal(Double.parseDouble("1.00")));
						}
					}
					
					NodeList priceNodes = invoiceLine.getElementsByTagName("cac:Price");
					if (priceNodes.getLength() > 0) {
					    Element priceElement = (Element) priceNodes.item(0);
					    NodeList priceAmountNodes = priceElement.getElementsByTagName("cbc:PriceAmount");
					    if (priceAmountNodes.getLength() > 0) {
					        Element priceAmountElement = (Element) priceAmountNodes.item(0);
					        String precioUnitarioSinIGV = priceAmountElement.getTextContent();
							items.setMtoValUnitario(BigDecimal.valueOf(Double.parseDouble(precioUnitarioSinIGV)));
					    }
					}
					
					NodeList pricingReferenceNodes = invoiceLine.getElementsByTagName("cac:PricingReference");
					if (pricingReferenceNodes.getLength() > 0) {
					    Element pricingReferenceElement = (Element) pricingReferenceNodes.item(0);
					    NodeList alternativeConditionPriceNodes = pricingReferenceElement.getElementsByTagName("cac:AlternativeConditionPrice");
					    if (alternativeConditionPriceNodes.getLength() > 0) {
					        Element alternativeConditionPriceElement = (Element) alternativeConditionPriceNodes.item(0);
					        NodeList priceAmountNodes = alternativeConditionPriceElement.getElementsByTagName("cbc:PriceAmount");
					        if (priceAmountNodes.getLength() > 0) {
					            Element priceAmountElement = (Element) priceAmountNodes.item(0);
					            String precioUnitario = priceAmountElement.getTextContent();
								items.setMtoImpTotal(BigDecimal.valueOf(Double.parseDouble(precioUnitario)).multiply(items.getCntItems()));
					        }
					    }
					}
					
					NodeList indFreeOfChargeIndicator = invoiceLine.getElementsByTagName("cbc:FreeOfChargeIndicator");
					if (indFreeOfChargeIndicator.getLength() > 0) {
						Element gratisElement = (Element) indFreeOfChargeIndicator.item(0);
						Boolean indGratis = Boolean.parseBoolean(gratisElement.getTextContent());
						if(indGratis)
						items.setMtoImpTotal(new BigDecimal("0.00"));
					}

					
					listItems.add(items);
				}
			}
		}
			return listItems;
		}
		
		
		private List<InformacionItems> obtenerInformacionItemsNotasCredito(Document document) {
			
			List<InformacionItems> listItems = new ArrayList<InformacionItems>();
			InformacionItems items = null;
			
			if(document!=null) {
			NodeList nlItems = document.getElementsByTagName("cac:CreditNoteLine");
			if (nlItems.getLength() > 0) {
				for (int i = 0; i < nlItems.getLength(); i++) {
					items = new InformacionItems();

					Element invoiceLine = (Element) nlItems.item(i);

					NodeList idList = invoiceLine.getElementsByTagName("cbc:ID");
					if (idList.getLength() > 0) {
						Element idElement = (Element) idList.item(0);
						String id = idElement.getTextContent();
						items.setNumFila(Integer.parseInt(id));
					}

					NodeList invoicedQuantity = invoiceLine.getElementsByTagName("cbc:CreditedQuantity");
					if (invoicedQuantity.getLength() > 0) {
						Element idElement = (Element) invoicedQuantity.item(0);
						String cantidad = idElement.getTextContent();
						try {
							if (cantidad == null || "".equals(cantidad) || "0".equals(cantidad)) {
								items.setCntItems(new BigDecimal("1.00"));
							} else {
								items.setCntItems(new BigDecimal(cantidad));
							}
						} catch(Exception e) {
							items.setCntItems(new BigDecimal(Double.parseDouble("1.00")));
						}
					}
					
					NodeList priceNodes = invoiceLine.getElementsByTagName("cac:Price");
					if (priceNodes.getLength() > 0) {
					    Element priceElement = (Element) priceNodes.item(0);
					    NodeList priceAmountNodes = priceElement.getElementsByTagName("cbc:PriceAmount");
					    if (priceAmountNodes.getLength() > 0) {
					        Element priceAmountElement = (Element) priceAmountNodes.item(0);
					        String precioUnitarioSinIGV = priceAmountElement.getTextContent();
							items.setMtoValUnitario(BigDecimal.valueOf(Double.parseDouble(precioUnitarioSinIGV)));
					    }
					}
					
					NodeList pricingReferenceNodes = invoiceLine.getElementsByTagName("cac:PricingReference");
					if (pricingReferenceNodes.getLength() > 0) {
					    Element pricingReferenceElement = (Element) pricingReferenceNodes.item(0);
					    NodeList alternativeConditionPriceNodes = pricingReferenceElement.getElementsByTagName("cac:AlternativeConditionPrice");
					    if (alternativeConditionPriceNodes.getLength() > 0) {
					        Element alternativeConditionPriceElement = (Element) alternativeConditionPriceNodes.item(0);
					        NodeList priceAmountNodes = alternativeConditionPriceElement.getElementsByTagName("cbc:PriceAmount");
					        if (priceAmountNodes.getLength() > 0) {
					            Element priceAmountElement = (Element) priceAmountNodes.item(0);
					            String precioUnitario = priceAmountElement.getTextContent();
								items.setMtoImpTotal(BigDecimal.valueOf(Double.parseDouble(precioUnitario)).multiply(items.getCntItems()));
					        }
					    }
					}

					
					listItems.add(items);
				}
			}
		}
			return listItems;
		}
		
		
		private List<InformacionItems> obtenerInformacionItemsNotasDebito(Document document) {
			
			List<InformacionItems> listItems = new ArrayList<InformacionItems>();
			InformacionItems items = null;
			
			if(document!=null) {
			NodeList nlItems = document.getElementsByTagName("cac:DebitNoteLine");
			if (nlItems.getLength() > 0) {
				for (int i = 0; i < nlItems.getLength(); i++) {
					items = new InformacionItems();

					Element invoiceLine = (Element) nlItems.item(i);

					NodeList idList = invoiceLine.getElementsByTagName("cbc:ID");
					if (idList.getLength() > 0) {
						Element idElement = (Element) idList.item(0);
						String id = idElement.getTextContent();
						items.setNumFila(Integer.parseInt(id));
					}

					NodeList invoicedQuantity = invoiceLine.getElementsByTagName("cbc:DebitedQuantity");
					if (invoicedQuantity.getLength() > 0) {
						Element idElement = (Element) invoicedQuantity.item(0);
						String cantidad = idElement.getTextContent();
						try {
							if (cantidad == null || "".equals(cantidad) || "0".equals(cantidad)) {
								items.setCntItems(new BigDecimal("1.00"));
							} else {
								items.setCntItems(new BigDecimal(cantidad));
							}
						} catch(Exception e) {
							items.setCntItems(new BigDecimal(Double.parseDouble("1.00")));
						}
					}
					
					NodeList priceNodes = invoiceLine.getElementsByTagName("cac:Price");
					if (priceNodes.getLength() > 0) {
					    Element priceElement = (Element) priceNodes.item(0);
					    NodeList priceAmountNodes = priceElement.getElementsByTagName("cbc:PriceAmount");
					    if (priceAmountNodes.getLength() > 0) {
					        Element priceAmountElement = (Element) priceAmountNodes.item(0);
					        String precioUnitarioSinIGV = priceAmountElement.getTextContent();
							items.setMtoValUnitario(BigDecimal.valueOf(Double.parseDouble(precioUnitarioSinIGV)));
					    }
					}
					
					NodeList pricingReferenceNodes = invoiceLine.getElementsByTagName("cac:PricingReference");
					if (pricingReferenceNodes.getLength() > 0) {
					    Element pricingReferenceElement = (Element) pricingReferenceNodes.item(0);
					    NodeList alternativeConditionPriceNodes = pricingReferenceElement.getElementsByTagName("cac:AlternativeConditionPrice");
					    if (alternativeConditionPriceNodes.getLength() > 0) {
					        Element alternativeConditionPriceElement = (Element) alternativeConditionPriceNodes.item(0);
					        NodeList priceAmountNodes = alternativeConditionPriceElement.getElementsByTagName("cbc:PriceAmount");
					        if (priceAmountNodes.getLength() > 0) {
					            Element priceAmountElement = (Element) priceAmountNodes.item(0);
					            String precioUnitario = priceAmountElement.getTextContent();
								items.setMtoImpTotal(BigDecimal.valueOf(Double.parseDouble(precioUnitario)).multiply(items.getCntItems()));
					        }
					    }
					}

					
					listItems.add(items);
				}
			}
		}
			return listItems;
		}
		
		
		private List<InformacionDocumentosRelacionados> obtenerDocsRel(Document document) {
			List<InformacionDocumentosRelacionados> listDocs = new ArrayList<>();
			InformacionDocumentosRelacionados docRel = null;
			if(document!=null) {
			NodeList nlGuias = document.getElementsByTagName("cac:DespatchDocumentReference");
			NodeList nlOtrosDocs = document.getElementsByTagName("cac:AdditionalDocumentReference");
			NodeList nlAnticipos = document.getElementsByTagName("cac:PrepaidPayment");
			
			if (nlGuias.getLength() > 0 || nlOtrosDocs.getLength() > 0) {
				int lengthArray = nlGuias.getLength() + nlOtrosDocs.getLength() + nlAnticipos.getLength();
				

				for (int j = 0, x = 0, y = 0, n = 0; j < lengthArray; j++) {
					
					Element element  = (j < nlGuias.getLength() ? (Element) nlGuias.item(x++)
							: j < (nlGuias.getLength() + nlOtrosDocs.getLength()) 
							? (Element) nlOtrosDocs.item(y++)
									: (Element) nlAnticipos.item(n++));
					
					docRel = new InformacionDocumentosRelacionados();
					NodeList idList = element.getElementsByTagName("cbc:ID");
					if (idList.getLength() > 0) {
						String[] serieNumCPE = idList.item(0).getTextContent().split("-");
						
						String numeroCPE = "";
						String serie ="";
						if(serieNumCPE.length==1){
							serie = "-";
							numeroCPE = serieNumCPE[0];				
						}
						if (serieNumCPE.length > 1) {
							serie = serieNumCPE[0];
							numeroCPE = serieNumCPE[1];
						}
						
						docRel.setNumSerieDocRel(serie);
						docRel.setNumDocRel(numeroCPE);
					}
					NodeList idCodigo = element.getElementsByTagName("cbc:DocumentTypeCode");
					if (idCodigo.getLength() > 0) {
						docRel.setCodCpeRel(idCodigo.item(0).getTextContent());
					}
					NodeList idDesc = element.getElementsByTagName("cbc:DocumentType");
					if (idDesc.getLength() > 0) {
						docRel.setDesCpeRel((idDesc.item(0).getTextContent()));
					}
					listDocs.add(docRel);
				}
			}
		}
			return listDocs;
		}

		public static String formatTenDecimal(String decimal) throws ParseException {
			// Formato para parsear el número con comas y puntos decimales
	        NumberFormat nf = new DecimalFormat("#,##0.############################################################", new DecimalFormatSymbols(Locale.ENGLISH));
	        // Formato para mantener todos los decimales y añadir dos ceros por defecto si no tiene decimales
	        DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.ENGLISH);
	        DecimalFormat df = new DecimalFormat("#,##0.00##########################################################", symbols);
	        // Parsear el número
	        Number number = nf.parse(decimal);
	        // Formatear el número conservando todos los decimales
	        return df.format(number.doubleValue());
		}
		
    private boolean verificarBonificacionEnItems(List<DetalleComprobante> detalle, List<Rubros> rubros) {

        //int totalItems = detalle.size();
        int totalBonificacionItems = 0;
        for (DetalleComprobante detalleAux : detalle) {

            if (!obtenerDescripcionRubroxitem(rubros, RubrosEnum.RUBRO_223, detalleAux.getDetalleComprobantePK().getNumFilaItem()).isEmpty()) {
                totalBonificacionItems++;
            }
        }

        if (totalBonificacionItems>0) {
            return true;
        }
        return false;
    }

    private String obtenerValorNodeDirecEstEmisor(Document newDocument) throws TransformerException, ParserConfigurationException, SAXException, IOException {

        String direccion = "";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList = newDocument.getElementsByTagName("cac:SellerSupplierParty");
            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:Party");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:PostalAddress");

                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        // Buscar el elemento cbc:RegistrationName
                        // Obtener el primer elemento cbc:RegistrationName
                        NodeList addressLineList = partyElement.getElementsByTagName("cac:AddressLine");
                        NodeList addressLineList1 = partyElement.getElementsByTagName("cbc:StreetName");
                        
                        if (addressLineList.getLength() > 0) {
                            Element addressLineElement = (Element) addressLineList.item(0);
                            direccion = addressLineElement.getTextContent();
                        }else if (addressLineList1.getLength() > 0){
                        	Element addressLineElement = (Element) addressLineList1.item(0);
                        	direccion = addressLineElement.getTextContent();
                        }


                    }

                }
            }
        }

        return direccion.replaceAll("[\\n\\t]", "").trim();
    }

    private String obtenerValorNodeDirecReceptor(Document newDocument,String codCpe) throws TransformerException, ParserConfigurationException, SAXException, IOException {
    	
        String direccion = "";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            if(codCpe.equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)){
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingSupplierParty");

            }else{
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingCustomerParty");

            }
            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:Party");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:PartyLegalEntity");

                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        // Buscar el elemento cbc:RegistrationName
                        // Obtener el primer elemento cbc:RegistrationName
                        NodeList addressLineList = partyElement.getElementsByTagName("cac:AddressLine");


                        if (addressLineList.getLength() > 0) {
                            Element addressLineElement = (Element) addressLineList.item(0);
                            direccion = addressLineElement.getTextContent();
                        }


                    }
                   
                    if("".equals(direccion)){
                    	
                    	NodeList postalAddressList = partyElement.getElementsByTagName("cac:PostalAddress");

                        if (postalAddressList.getLength() > 0) {

                            NodeList streetNameList = partyElement.getElementsByTagName("cbc:StreetName");


                            if (streetNameList.getLength() > 0) {
                                Element addressLineElement = (Element) streetNameList.item(0);
                                direccion = addressLineElement.getTextContent();
                            }


                        }
                    	
                    }

                }
            }
        }

        return direccion.replaceAll("[\\n\\t]", "").trim();
    }
    private String ObtenerValorLugarRecep(Document newDocument) throws TransformerException, ParserConfigurationException, SAXException, IOException{
    	
        String desRazonSocialEmis = "-";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList invoice;
            invoice = newDocument.getElementsByTagName("Invoice");
            if (invoice.getLength() > 0) {
                Element invoiceElement = (Element) invoice.item(0);
                NodeList accountingSupplierPartyList = invoiceElement.getElementsByTagName("cac:Delivery");
                if (accountingSupplierPartyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Delivery
                    Element deliveryElement = (Element) accountingSupplierPartyList.item(0);
                    Element parentElement = (Element) deliveryElement.getParentNode();

                    if(!parentElement.getNodeName().equals("cac:InvoiceLine")){
                        NodeList partyList = deliveryElement.getElementsByTagName("cac:DeliveryLocation");

                        if (partyList.getLength() > 0) {
                            // Obtener el primer elemento cac:Party
                            Element partyElement = (Element) partyList.item(0);

                            // Buscar el elemento cac:PartyLegalEntity
                            NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:Address");

                            if (partyLegalEntityList.getLength() > 0) {
                                // Obtener el primer elemento cac:PartyLegalEntity
                                Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);

                                // Buscar el elemento cbc:RegistrationName
                                NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cac:AddressLine");

                                if (registrationNameList.getLength() > 0) {
                                    // Obtener el primer elemento cbc:RegistrationName
                                    Element registrationNameElement = (Element) registrationNameList.item(0);
                                    NodeList registrationLineList = registrationNameElement.getElementsByTagName("cbc:Line");

                                    if (registrationLineList.getLength() > 0) {
                                        Element registrationNameLine = (Element) registrationLineList.item(0);
                                        String registrationNameValue = registrationNameLine.getTextContent();
                                        desRazonSocialEmis = registrationNameValue;

                                    }

                                    // Obtener el texto contenido en el elemento cbc:RegistrationName

                                }
                            }
                        }
                    }
                    // Buscar el elemento cac:Party

                }
            }
        }
        return desRazonSocialEmis.replaceAll("[\\n\\t]", "").trim();
    }
    private String obtenerValorNodeRazonSocialReceptor(Document newDocument,String codCpe) throws TransformerException, ParserConfigurationException, SAXException, IOException {

        String desRazonSocialEmis = "-";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            if(codCpe.equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)){
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingSupplierParty");

            }else{
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingCustomerParty");

            }
            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:Party");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:PartyLegalEntity");
                    NodeList partyLegalEntityList1 = partyElement.getElementsByTagName("cac:PartyTaxScheme");

                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);

                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cbc:RegistrationName");

                        if (registrationNameList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            Element registrationNameElement = (Element) registrationNameList.item(0);

                            // Obtener el texto contenido en el elemento cbc:RegistrationName
                            String registrationNameValue = registrationNameElement.getTextContent();

                            desRazonSocialEmis = registrationNameValue;
                        }
                    }else if(partyLegalEntityList1.getLength() > 0){

                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList1.item(0);

                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cbc:RegistrationName");

                        if (registrationNameList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            Element registrationNameElement = (Element) registrationNameList.item(0);

                            // Obtener el texto contenido en el elemento cbc:RegistrationName
                            String registrationNameValue = registrationNameElement.getTextContent();

                            desRazonSocialEmis = registrationNameValue;
                        }
                    
                    }
                }
            }
        }
        return desRazonSocialEmis.replaceAll("[\\n\\t]", "").trim();
    }
    
    private BigDecimal obtenerMontoAnticipoXMLPortal(Document newDocument) throws TransformerException, ParserConfigurationException, SAXException, IOException {

        BigDecimal montoAnticipo = BigDecimal.ZERO;
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:LegalMonetaryTotal");
               
                    if (accountingSupplierPartyList.getLength() > 0) {
                        // Obtener el primer elemento cac:LegalMonetaryTotal
                        Element partyLegalEntityElement = (Element) accountingSupplierPartyList.item(0);

                        // Buscar el elemento cbc:PrepaidAmount
                        NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cbc:PrepaidAmount");

                        if (registrationNameList.getLength() > 0) {
                            // Obtener el primer elemento cbc:PrepaidAmount
                            Element registrationNameElement = (Element) registrationNameList.item(0);

                            // Obtener el texto contenido en el elemento cbc:PrepaidAmount
                            String registrationNameValue = registrationNameElement.getTextContent();
                            BigDecimal valorMontoBigDecimal = new BigDecimal(registrationNameValue);
                            montoAnticipo = valorMontoBigDecimal;
                        }
                    }
                
            
        }
        return montoAnticipo;
    }
    
    private BigDecimal obtenerMontoOtrosCargosXMLGEM(Document newDocument) throws TransformerException, ParserConfigurationException, SAXException, IOException {

        BigDecimal montoOtrosCargos = BigDecimal.ZERO;
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:LegalMonetaryTotal");
               
                    if (accountingSupplierPartyList.getLength() > 0) {
                        // Obtener el primer elemento cac:LegalMonetaryTotal
                        Element partyLegalEntityElement = (Element) accountingSupplierPartyList.item(0);

                        // Buscar el elemento cbc:ChargeTotalAmount
                        NodeList registrationNameList = partyLegalEntityElement.getElementsByTagName("cbc:ChargeTotalAmount");

                        if (registrationNameList.getLength() > 0) {
                            // Obtener el primer elemento cbc:ChargeTotalAmount
                            Element registrationNameElement = (Element) registrationNameList.item(0);

                            // Obtener el texto contenido en el elemento cbc:ChargeTotalAmount
                            String registrationNameValue = registrationNameElement.getTextContent();
                            BigDecimal valorMontoBigDecimal = new BigDecimal(registrationNameValue);
                            montoOtrosCargos = valorMontoBigDecimal;
                        }
                    }
                
            
        }
        return montoOtrosCargos;
    }

    private String obtenerValorUbigeo(Document newDocument,String codCpe) throws TransformerException, ParserConfigurationException, SAXException, IOException {

        String ubigeo = "";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            NodeList accountingSupplierPartyList1;
            if(codCpe.equals(Constantes.COD_CPE_LIQUIDACION_COMPRA)){
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingCustomerParty");
                accountingSupplierPartyList1 = newDocument.getElementsByTagName("cac:DeliveryTerms");
            }else{
                accountingSupplierPartyList = newDocument.getElementsByTagName("cac:AccountingSupplierParty");
                accountingSupplierPartyList1 = newDocument.getElementsByTagName("cac:DeliveryTerms");
            }
            
            
            
            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:Party");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:PartyLegalEntity");

                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);

                        // Buscar el elemento cbc:RegistrationName
                        NodeList registrationAddressList = partyLegalEntityElement.getElementsByTagName("cac:RegistrationAddress");

                        if (registrationAddressList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            NodeList provinciaLineList = partyLegalEntityElement.getElementsByTagName("cbc:CityName");
                            NodeList departamentoLineList = partyLegalEntityElement.getElementsByTagName("cbc:CountrySubentity");
                            NodeList distritoLineList = partyLegalEntityElement.getElementsByTagName("cbc:District");


                            if (departamentoLineList.getLength() > 0) {
                                Element addressLineElement = (Element) departamentoLineList.item(0);

                                ubigeo = ubigeo.concat(addressLineElement.getTextContent() + "-");

                            }
                            if (provinciaLineList.getLength() > 0) {
                                Element addressLineElement = (Element) provinciaLineList.item(0);

                                ubigeo = ubigeo.concat(addressLineElement.getTextContent() + "-");

                            }

                            if (distritoLineList.getLength() > 0) {
                                Element addressLineElement = (Element) distritoLineList.item(0);

                                ubigeo = ubigeo.concat(addressLineElement.getTextContent());

                            }
                            // Obtener el texto contenido en el elemento cbc:RegistrationName

                        }
                    }
                }
                
                
                if ("".equals(ubigeo)){
                	
                    // Obtener el primer elemento cac:Party
                    Element postalAddressElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList postalAddressList = postalAddressElement.getElementsByTagName("cac:PostalAddress");

                        if (postalAddressList.getLength() > 0) {
                            // Obtener el primer elemento cbc:RegistrationName
                            NodeList provinciaLineList = postalAddressElement.getElementsByTagName("cbc:CityName");
                            NodeList departamentoLineList = postalAddressElement.getElementsByTagName("cbc:CountrySubentity");
                            NodeList distritoLineList = postalAddressElement.getElementsByTagName("cbc:District");


                            if (departamentoLineList.getLength() > 0) {
                                Element addressLineElement = (Element) departamentoLineList.item(0);

                                ubigeo = ubigeo.concat(addressLineElement.getTextContent() + "-");

                            }
                            if (provinciaLineList.getLength() > 0) {
                                Element addressLineElement = (Element) provinciaLineList.item(0);

                                ubigeo = ubigeo.concat(addressLineElement.getTextContent() + "-");

                            }

                            if (distritoLineList.getLength() > 0) {
                                Element addressLineElement = (Element) distritoLineList.item(0);

                                ubigeo = ubigeo.concat(addressLineElement.getTextContent());

                            }
                            // Obtener el texto contenido en el elemento cbc:RegistrationName

                    }
                
                	
                	
                }
                
            } else if(accountingSupplierPartyList1.getLength() > 0){
       	
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList1.item(0);

                // Buscar el elemento cac:Party
                NodeList partyList = accountingSupplierPartyElement.getElementsByTagName("cac:DeliveryLocation");

                if (partyList.getLength() > 0) {
                    // Obtener el primer elemento cac:Party
                    Element partyElement = (Element) partyList.item(0);

                    // Buscar el elemento cac:PartyLegalEntity
                    NodeList partyLegalEntityList = partyElement.getElementsByTagName("cac:Address");
                    
                    if (partyLegalEntityList.getLength() > 0) {
                        // Obtener el primer elemento cac:PartyLegalEntity
                        Element partyLegalEntityElement = (Element) partyLegalEntityList.item(0);
                        // Buscar el elemento cbc:RegistrationName
                        NodeList provinciaLineList = partyLegalEntityElement.getElementsByTagName("cbc:CityName");
                        NodeList departamentoLineList = partyLegalEntityElement.getElementsByTagName("cbc:CountrySubentity");
                        NodeList distritoLineList = partyLegalEntityElement.getElementsByTagName("cbc:District");


                        if (departamentoLineList.getLength() > 0) {
                            Element addressLineElement = (Element) departamentoLineList.item(0);

                            ubigeo = ubigeo.concat(addressLineElement.getTextContent() + "-");

                        }
                        if (provinciaLineList.getLength() > 0) {
                            Element addressLineElement = (Element) provinciaLineList.item(0);

                            ubigeo = ubigeo.concat(addressLineElement.getTextContent() + "-");

                        }

                        if (distritoLineList.getLength() > 0) {
                            Element addressLineElement = (Element) distritoLineList.item(0);

                            ubigeo = ubigeo.concat(addressLineElement.getTextContent());

                        }
                    }
                }       	
        	} else {
                ubigeo = "-";

            }
        }
        return ubigeo.replaceAll("[\\n\\t]", "").trim();
    }
    
    
    
    private String obtenerValorPlacaFactura(Document newDocument) throws TransformerException, ParserConfigurationException, SAXException, IOException {

        String placa = "-";
        if (Objects.nonNull(newDocument)) {
        	newDocument.getDocumentElement().normalize();
            NodeList accountingSupplierPartyList;
            accountingSupplierPartyList = newDocument.getElementsByTagName("cac:RoadTransport");

            if (accountingSupplierPartyList.getLength() > 0) {
                // Obtener el primer elemento cac:AccountingSupplierParty
                Element accountingSupplierPartyElement = (Element) accountingSupplierPartyList.item(0);

                // Buscar el elemento cac:Party
                NodeList licensePlate = accountingSupplierPartyElement.getElementsByTagName("cbc:LicensePlateID");
                        if (licensePlate.getLength() > 0) {
                            // Obtener el primer elemento cbc:LicensePlateID
                            Element registrationNameElement = (Element) licensePlate.item(0);

                            // Obtener el texto contenido en el elemento cbc:RegistrationName
                            String registrationNameValue = registrationNameElement.getTextContent();

                            placa = registrationNameValue;
                        }
            }
        }
        return placa.replaceAll("[\\n\\t]", "").trim();
    }
    
    
    
    
		public File recuperaComprobanteXmlNube(ArchivoRequestDTO archivoRequestDTO) {
			utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "ComprobantesRepositoryImpl.recuperaComprobanteXmlNube - INICIO");
			ArchivoResponseDTO archivoResponseDTO = new ArchivoResponseDTO();
			File newFileXml = null;
			try {
				archivoResponseDTO = recuperarComprobanteNube(archivoRequestDTO,Constantes.TIPODOC_CPE_NUBE_XML);
				if(archivoResponseDTO!=null){
					newFileXml = new File(archivoResponseDTO.getNomArchivo());
					FileUtils.writeByteArrayToFile(newFileXml, archivoResponseDTO.getValArchivo());
				}
				utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, "ComprobantesRepositoryImpl.recuperaComprobanteXmlNube - FIN");
			} catch (Exception e) {
				utilLog.imprimirLog(ConstantesUtils.LEVEL_ERROR, "ComprobantesRepositoryImpl.recuperaComprobanteXmlNube Error: " + e.getMessage());
				return newFileXml;
			}
			return newFileXml;
		}
		
		
		@Override
		public ArchivoResponseDTO recuperarComprobanteNube(ArchivoRequestDTO archivoRequestDTO, String tipoDoc) throws Exception{
			
			ArchivoResponseDTO archivoResponseDTO = null;
			utilLog.imprimirLogKibana(ConstantesUtils.LEVEL_DEBUG, "archivoRequestDTO: " + archivoRequestDTO.toString());
			if(EnumTipoComp.byKey(archivoRequestDTO.getCodCpe()) != null) {
				// obtener rubros, cod_cpe ya llega con el valor correcto 01 03 07 08 etc
				RubrosPK rubrosPk = new RubrosPK();
				rubrosPk.setNumSerieCpe(archivoRequestDTO.getNumSerieCpe());
				rubrosPk.setCodCpe(archivoRequestDTO.getCodCpe());
				rubrosPk.setNumRuc(archivoRequestDTO.getNumRucEmisor());
				rubrosPk.setNumCpe(archivoRequestDTO.getNumCpe().intValue());
				rubrosPk.setCodRubro(String.valueOf(Constantes.COD_RUBRO_CPE_NUBE));
				
				boolean isResumen = false;
				if (archivoRequestDTO.getCodCpeOri().equals(Constantes.COD_RESUMEN_BOLETA_CREDITO)
						|| archivoRequestDTO.getCodCpeOri().equals(Constantes.COD_RESUMEN_BOLETA_DEBITO)
						|| archivoRequestDTO.getCodCpeOri().equals(Constantes.COD_RESUMEN_BOLETA)) {
					isResumen = true;
				}
				List<Rubros> rubros = new ArrayList<>();
				if(isResumen) {
					rubros = rubrosRepository.obtenerRubrosResumen(rubrosPk);
				}else {
					rubros = rubrosRepository.obtenerRubrosIdx(rubrosPk);
				}
				
				if(rubros != null && !rubros.isEmpty()) {
					String guid = rubros.get(0).getDesDetalleRubro();
					utilLog.imprimirLogKibana(ConstantesUtils.LEVEL_DEBUG, "guid: " + rubros.get(0).toString());
					byte[] comprobanteXml = consultaCPEService.procesarComprobanteNubev(guid,tipoDoc);
					if(comprobanteXml.length>0) {
						
						//Grabacion NFS
						
						byte[] comprobanteXmlUnZip=decompress(comprobanteXml);
											
						ComprobantePK comprobantePK = new ComprobantePK();
			    		comprobantePK.setNumCpe(Integer.valueOf(rubrosPk.getNumCpe()));
			    		comprobantePK.setNumSerieCpe(rubrosPk.getNumSerieCpe());
			    		comprobantePK.setCodCpe(rubrosPk.getCodCpe());
			    		comprobantePK.setNumRuc(rubrosPk.getNumRuc());
			    		String extArchivo="zip";
			    		Calendar calendario = Calendar.getInstance();
			    		calendario.setTime(comprobanteRepository.obtenerFechaEmision(comprobantePK));
			    		String annio = String.valueOf(calendario.get(Calendar.YEAR));
			    		String mes = String.format("%02d", calendario.get(Calendar.MONTH) + 1);
			    		String dia = String.format("%02d", calendario.get(Calendar.DAY_OF_MONTH));
			    		char ultimoDigitoRuc = comprobantePK.getNumRuc().charAt(comprobantePK.getNumRuc().length() - 1);
			    		String cuatroPenultimosRuc=comprobantePK.getNumRuc().substring(6, 10);
			    		String ruta=null;
			    		ruta= annio +"/"+mes+"/"+dia+"/"+ultimoDigitoRuc+"/"+cuatroPenultimosRuc+"/"+comprobantePK.getNumRuc()+"/"+comprobantePK.getCodCpe()+"/0";
			    		String nomArchivoGrab=rubrosPk.getNumRuc() + "-"+ rubrosPk.getCodCpe() + "-" + rubrosPk.getNumSerieCpe() + "-" + rubrosPk.getNumCpe();
			    		utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,
			        			"XmlComprobanteServiceImpl.recuperaComprobanteXmlService - ruta: "+ruta);
			    		String textoJsonGrab=null;
			    		String base64Zip = null;
			    		// Codificar el valor base64
    	    			if(System.getProperty("java.version").startsWith("1.7")) {
    	    	            // Utilizando la codificación para Java 6 y posteriores
    	    				sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
    	    	            try {
    	    	            	base64Zip = encoder.encode(compress(comprobanteXmlUnZip,nomArchivoGrab+".XML")).replaceAll("\\s", ""); 
    	    	            } catch (Exception e) {
    	    	                e.printStackTrace();
    	    	            }
    	    	        } else {
    	    	            // Utilizando la codificación para Java 8 y posteriores
    	    	        	base64Zip = Base64.getEncoder().encodeToString(compress(comprobanteXmlUnZip,nomArchivoGrab+".XML")).replaceAll("\\s", ""); 
    	    	        }
    					textoJsonGrab = "{\"base64\":\""+base64Zip+"\",\"ruta\":\""+ruta+"\",\"nomArchivo\":\"" + nomArchivoGrab + "\",\"extArchivo\":\""+extArchivo+"\"}";
    					utilLog.imprimirLogKibana(ConstantesUtils.LEVEL_DEBUG, "JSON de grabacion ==========> : " + textoJsonGrab);
    				
    					if (textoJsonGrab != null) {
						String res = grabarArchivoNFS(textoJsonGrab);
    					}	
    					//Fin cambio NFS
						
						String nombreArchivo = archivoRequestDTO.getNumRucEmisor() + "-" + archivoRequestDTO.getCodCpe()
						+ "-"+ archivoRequestDTO.getNumSerieCpe() + "-" + archivoRequestDTO.getNumCpe() + "-" + tipoDoc + Constantes.EXTENSION_ZIP;
						archivoResponseDTO = new ArchivoResponseDTO();
						archivoResponseDTO.setNomArchivo(nombreArchivo);
						archivoResponseDTO.setValArchivo(comprobanteXml);
					}
				}else {
					utilLog.imprimirLogKibana(ConstantesUtils.LEVEL_DEBUG, "No se encontro Rubros Nube para los filtros enviados: " + rubrosPk.toString());
				}
			}
			
			return archivoResponseDTO;
		}
		
		
		public String grabarArchivoNFS(String textoJsonGrab) {
			String resp = "";

			// JSON que quieres enviar
			String jsonInputString = textoJsonGrab;
			HttpURLConnection con = null;
			BufferedReader br = null;
			OutputStream os = null;
			try {
				ParametriaRepositoryImpl parametriaRepository = new ParametriaRepositoryImpl();
				T01paramPK t01paramPK = new T01paramPK();
				t01paramPK.setNumero("967");
				t01paramPK.setTipo("D");
				t01paramPK.setArgumento("INTRES16152602");
				T01param resultado = parametriaRepository.obtenerParametroByPk(t01paramPK);
				String servicioGraba=null;
				if (resultado != null) {
					servicioGraba=resultado.getFuncion().substring(0, 119).trim();
					String servicioConsultaEstado=resultado.getFuncion().substring(124, 125).trim();
					if("0".equals(servicioConsultaEstado)) {				
						throw new RuntimeException("Servicio Web se encuentra inhabilitado.WS:"+"INTRES16152602");}
				} else {
					return null;
				}

				URL urlObj = new URL(servicioGraba);
				con = (HttpURLConnection) urlObj.openConnection();
				con.setRequestMethod("POST");
				con.setRequestProperty("Content-Type", "application/json");
				con.setDoOutput(true);
				
				os = con.getOutputStream();
				byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
				
				br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
				StringBuilder response = new StringBuilder();
				String responseLine;
				
				while ((responseLine = br.readLine()) != null) {
					response.append(responseLine.trim());
				}
				resp = response.toString();
				
				return resp;
			} catch (Exception e) {
				utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"grabarNFS - Exception: "+e);
				return null;
			} finally {
				if (os != null) {
					try {
						os.close();
					} catch (Exception e) 
					{ utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"grabarNFS - Exception close os: "+e);}
				}
				if (br != null) {
					try {
						br.close();
					} catch (Exception e) {
						utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG,"grabarNFS - Exception close br: "+e);
					}
				}
				if (con != null) {
					con.disconnect();
				}
			}
		}


                
 private Comprobantes llenarDatosFacturaBoletaMasivo(Comprobante comprobante,ParametrosSer parametrosSer,Date fecEmision) 
    				throws IOException, ParserConfigurationException, SAXException, TransformerException {
        Comprobantes comprobanteIndivual = new Comprobantes();
        DatosEmisor datosEmisor = new DatosEmisor();
        DatosReceptor datosReceptor = new DatosReceptor();
        datosEmisor.setNumRuc(comprobante.getComprobantePK().getNumRuc());
        datosEmisor.setDesRazonSocialEmis(obtenerRazonSocial(comprobante.getComprobantePK().getNumRuc()));
        datosReceptor.setCodDocIdeRecep(comprobante.getCodDocIdeRecep());
        datosReceptor.setNumDocIdeRecep(comprobante.getNumDocIdeRecep());
        if(comprobante.getCodDocIdeRecep().trim().equals("6")||comprobante.getCodDocIdeRecep().trim().equals("06")){
        datosReceptor.setDesRazonSocialRecep(obtenerRazonSocial(comprobante.getNumDocIdeRecep()));
        }else{
        datosReceptor.setDesRazonSocialRecep(comprobante.getDesNombreRecep());
        }
        comprobanteIndivual.setDatosEmisor(datosEmisor);
        comprobanteIndivual.setDatosReceptor(datosReceptor);
        
        comprobanteIndivual.setNumSerie(comprobante.getComprobantePK().getNumSerieCpe());
        comprobanteIndivual.setCodCpe(comprobante.getComprobantePK().getCodCpe());
        comprobanteIndivual.setNumCpe(comprobante.getComprobantePK().getNumCpe());

        if (!comprobante.getCodDocIdeRecep().isEmpty() && !comprobante.getCodDocIdeRecep().equals("-")) {
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(comprobante.getCodDocIdeRecep().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
        } else {
        		comprobanteIndivual.setDesTipoCpe("-");
        }
        if("00".endsWith(comprobante.getCodDocIdeRecep().trim()) || "0".endsWith(comprobante.getCodDocIdeRecep().trim())){
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
    	}
        comprobanteIndivual.setFecEmision(comprobante.getFecEmision());
        comprobanteIndivual.setIndEstadoCpe(comprobante.getIndEstado());
        comprobanteIndivual.setIndProcedencia(comprobante.getIndProcedencia());
        
        DocumentoRelacionadoPK docRelacionadoPk = new DocumentoRelacionadoPK();
        docRelacionadoPk.setNumDocRel(String.valueOf(comprobante.getComprobantePK().getNumCpe()));
        docRelacionadoPk.setCodDocRel(comprobante.getComprobantePK().getCodCpe());
        docRelacionadoPk.setNumSerieDocRel(comprobante.getComprobantePK().getNumSerieCpe());
        docRelacionadoPk.setNumRucRel(comprobante.getComprobantePK().getNumRuc());

        List<DocumentoRelacionado> notas = Optional
                .ofNullable(docRelacionadoRepository.obtenerNotas(docRelacionadoPk))
                .orElse(null);
        List<InformacionNotas> lstInformacionNotas = new ArrayList<>();
        int contador = 1;

        if (Objects.nonNull(notas)) {
            for (DocumentoRelacionado documentoRel : notas) {
                InformacionNotas informacionNota = new InformacionNotas();
                informacionNota.setNumCorrel(contador);
                informacionNota.setFecEmision(documentoRel.getFecEmision());
                informacionNota.setMtoTotal(documentoRel.getMtoTotal());
                informacionNota.setCodTipNota(ComprobanteUtilBean.obtenerTipoNota(documentoRel.getDocumentoRelacionadoPK().codCpe));
                informacionNota.setNumCpe(documentoRel.getDocumentoRelacionadoPK().getNumCpe());
                informacionNota.setNumSerie(documentoRel.getDocumentoRelacionadoPK().getNumSerieCpe());
              informacionNota.setCodTipCpe(documentoRel.getCodTipoCpe());
                lstInformacionNotas.add(informacionNota);
                contador = contador + 1;
            }
            if (!notas.isEmpty()) {
                comprobanteIndivual.setInformacionNotas(lstInformacionNotas);
            }
        }

        return  comprobanteIndivual;
    }


    private String obtenerRazonSocial(String numRuc) {
        String desRazonSocialEmis = "";
        try {
            Contribuyentes contribuyente = contribuyenteRepository.obtenerContribuyente(numRuc.trim());
            if (contribuyente != null) {
                desRazonSocialEmis = contribuyente.getDesNombre().trim();
            }
        } catch (Exception ex) {
            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, ex.getMessage());
        }
        return desRazonSocialEmis;
    }

    private String obtenerDireccion(String numRuc) {
        String desRazonSocialEmis = "";
        try {
            Contribuyentes contribuyente = contribuyenteRepository.obtenerContribuyente(numRuc.trim());
            if (contribuyente != null) {
                desRazonSocialEmis = contribuyente.getUbigeo().getDesNomZon();
            }
        } catch (Exception ex) {
            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, ex.getMessage());
        }
        return desRazonSocialEmis;
    }

    private String obtenerUbigeo(String numRuc) {
        String ubigeoStr = "";
        try {
            Contribuyentes contribuyente = contribuyenteRepository.obtenerContribuyente(numRuc.trim());
            if (contribuyente != null) {
                ubigeoStr = ubigeoStr
                        .concat(contribuyente.getUbigeo().getDesDepartamento() + "-")
                        .concat(contribuyente.getUbigeo().getDesProvincia() + "-")
                        .concat(contribuyente.getUbigeo().getDesDistrito());
            }
        } catch (Exception ex) {
            utilLog.imprimirLog(ConstantesUtils.LEVEL_DEBUG, ex.getMessage());
        }
        return ubigeoStr;
    }
    
    private Comprobantes llenarDatosNotasMasivo(Comprobante comprobante,ParametrosSer parametrosSer,Date fecEmision) 
    				throws IOException, ParserConfigurationException, SAXException, TransformerException {
        Comprobantes comprobanteIndivual= new Comprobantes();
        DatosEmisor datosEmisor = new DatosEmisor();
        DatosReceptor datosReceptor = new DatosReceptor();
        datosEmisor.setNumRuc(comprobante.getComprobantePK().getNumRuc());
        datosEmisor.setDesRazonSocialEmis(obtenerRazonSocial(comprobante.getComprobantePK().getNumRuc()));
        datosReceptor.setCodDocIdeRecep(comprobante.getCodDocIdeRecep());
        datosReceptor.setNumDocIdeRecep(comprobante.getNumDocIdeRecep());
        if(comprobante.getCodDocIdeRecep().trim().equals("6")||comprobante.getCodDocIdeRecep().trim().equals("06")){
        datosReceptor.setDesRazonSocialRecep(obtenerRazonSocial(comprobante.getNumDocIdeRecep()));
        }else{
        datosReceptor.setDesRazonSocialRecep(comprobante.getDesNombreRecep());
        }
        comprobanteIndivual.setDatosEmisor(datosEmisor);
        comprobanteIndivual.setDatosReceptor(datosReceptor);

        comprobanteIndivual.setNumSerie(comprobante.getComprobantePK().getNumSerieCpe());
        comprobanteIndivual.setCodCpe(comprobante.getComprobantePK().getCodCpe());
        comprobanteIndivual.setNumCpe(comprobante.getComprobantePK().getNumCpe());
  
        if (!comprobante.getCodDocIdeRecep().isEmpty() && !comprobante.getCodDocIdeRecep().equals("-")) {
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(comprobante.getCodDocIdeRecep().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

        } else {
        		comprobanteIndivual.setDesTipoCpe("-");
        }
        if("00".endsWith(comprobante.getCodDocIdeRecep().trim()) || "0".endsWith(comprobante.getCodDocIdeRecep().trim())){
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
    	}
        comprobanteIndivual.setFecEmision(comprobante.getFecEmision());
        comprobanteIndivual.setIndEstadoCpe(comprobante.getIndEstado());
        comprobanteIndivual.setIndProcedencia(comprobante.getIndProcedencia());        
               
        return  comprobanteIndivual;
    }
    
    private Comprobantes llenarDatosLiquidacionMasivo(Comprobante comprobante,ParametrosSer parametrosSer,Date fecEmision) throws IOException, ParserConfigurationException, SAXException, TransformerException {
        Comprobantes comprobanteIndivual= new Comprobantes();
        DatosEmisor datosEmisor = new DatosEmisor();
        DatosReceptor datosReceptor = new DatosReceptor();
        datosEmisor.setNumRuc(comprobante.getComprobantePK().getNumRuc());
        datosEmisor.setDesRazonSocialEmis(obtenerRazonSocial(comprobante.getComprobantePK().getNumRuc()));
        datosReceptor.setCodDocIdeRecep(comprobante.getCodDocIdeRecep());
        datosReceptor.setNumDocIdeRecep(comprobante.getNumDocIdeRecep());
        if(comprobante.getCodDocIdeRecep().trim().equals("6")||comprobante.getCodDocIdeRecep().trim().equals("06")){
        datosReceptor.setDesRazonSocialRecep(obtenerRazonSocial(comprobante.getNumDocIdeRecep()));
        }else{
        datosReceptor.setDesRazonSocialRecep(comprobante.getDesNombreRecep());
        }
        comprobanteIndivual.setDatosEmisor(datosEmisor);
        comprobanteIndivual.setDatosReceptor(datosReceptor);

        comprobanteIndivual.setNumSerie(comprobante.getComprobantePK().getNumSerieCpe());
        comprobanteIndivual.setCodCpe(comprobante.getComprobantePK().getCodCpe());
        comprobanteIndivual.setNumCpe(comprobante.getComprobantePK().getNumCpe());

        if (!comprobante.getCodDocIdeRecep().isEmpty() && !comprobante.getCodDocIdeRecep().equals("-")) {
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals(comprobante.getCodDocIdeRecep().replaceAll("^0+", ""))).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));

        } else {
        		comprobanteIndivual.setDesTipoCpe("-");
        }
        if("00".endsWith(comprobante.getCodDocIdeRecep().trim()) || "0".endsWith(comprobante.getCodDocIdeRecep().trim())){
            comprobanteIndivual.setDesTipoCpe(parametrosSer.getTiposDocumento().stream().filter(tiposDocumento -> tiposDocumento.getCodDocIdeRecep().equals("0")).findFirst().map(TiposDocumento::getDesDocIdeRecep).orElse("-"));
    	}
        comprobanteIndivual.setFecEmision(comprobante.getFecEmision());
        comprobanteIndivual.setIndEstadoCpe(comprobante.getIndEstado());
        comprobanteIndivual.setIndProcedencia(comprobante.getIndProcedencia());
   
        return  comprobanteIndivual;
    }
    
    public String obtenerDatosDireccionPei(String ubigeo) {
        String dirDatos = "-";
        if (ubigeo != null && !ubigeo.isEmpty() && ubigeo.length() >= 6) { // Verifica que la longitud mínima sea 6
            String tipoUbigeo = "031";
            String departamento = ubigeoRepository.obtenerDepartamentos(tipoUbigeo, ubigeo.substring(0, 2).trim() + "0000").stream().findFirst().map(UbigeoVO::getDesParam).orElse("-");
            String provincia = ubigeoRepository.obtenerProvinvias(tipoUbigeo, ubigeo.substring(0, 4).trim() + "00").stream().findFirst().map(UbigeoVO::getDesParam).orElse("-");
            String distrito = ubigeoRepository.obtenerDistritos(tipoUbigeo, ubigeo.substring(0, 2).trim() + "0000", ubigeo.substring(0, 4).trim() + "00", ubigeo.trim()).stream().findFirst().map(UbigeoVO::getDesParam).orElse("-");

            dirDatos = departamento + "-" + provincia + "-" + distrito;
        } 
        return dirDatos;
    }

    

}



package pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.ws.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ArchivoNfsResponseDTO {
	private String cod;
    private String msg;
    private Result[] results;

    // Getters and setters
    public String getCod() {
        return cod;
    }

    public void setCod(String cod) {
        this.cod = cod;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Result[] getResults() {
        return results;
    }

    public void setResults(Result[] results) {
        this.results = results;
    }

    public static class Result {
        private String nomArchivo;
        private String base64;

        // Getters and setters
        public String getNomArchivo() {
            return nomArchivo;
        }

        public void setNomArchivo(String nomArchivo) {
            this.nomArchivo = nomArchivo;
        }

        public String getBase64() {
            return base64;
        }

        public void setBase64(String base64) {
            this.base64 = base64;
        }
    }

}


package pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdrecauda.dao.jpa;

import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdrecauda.T01param;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdrecauda.T01paramPK;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.basebdrecauda.dao.ParametriaRepository;
import pe.gob.sunat.contribuyentems.servicio.consultacpe.consulta.facbolliq.main.config.EntityManagerRecauda;
import pe.gob.sunat.tecnologiams.arquitectura.framework.jpa.dao.AbstractDao;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParametriaRepositoryImpl extends AbstractDao<T01param, T01paramPK> implements ParametriaRepository {
	
	private EntityManagerRecauda entityManagerRecauda;
	
	public ParametriaRepositoryImpl() {
		this.entityManagerRecauda = new EntityManagerRecauda();
	}

	@Override
	public EntityManager buildEntityManager() {
		return entityManagerRecauda.getCentral();
	}

	protected final Log log = LogFactory.getLog(getClass());

	@Override
	public Class<T01param> provideEntityClass() {
		return T01param.class;
	}

	@Override
	public T01param obtenerParametroByPk(T01paramPK t01paramPK) {
		
		String querySql = " select t01_numero,t01_tipo,t01_argumento,t01_funcion,t01_user,t01_factualiz,t01_hora from t01param t ";
		querySql = querySql + " where t.t01_numero = ? and t.t01_tipo = ? and t.t01_argumento = ? ";
		log.debug("query=> " + querySql);
		T01param element = null;
		try {
			EntityManager entityManager = entityManagerRecauda.getCentral();
			Query query = entityManager.createNativeQuery(querySql, T01param.class);
			query.setParameter(1, t01paramPK.getNumero());
			query.setParameter(2, t01paramPK.getTipo());
			query.setParameter(3, t01paramPK.getArgumento());
			
			element = (T01param) query.getSingleResult();
		} catch (NoResultException nre) {
			log.error(nre.getMessage());
			element = null;
		} catch (Exception e) {
			log.error(e.getMessage());
			element = null;
		}
		return element;
	}

}
