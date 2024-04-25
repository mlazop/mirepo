package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.ws.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.bean.MensajeRespuesta;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.bean.PartialError;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.dto.MensajeArchivoDto;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.dto.MensajeDatosDto;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.services.IProcesarDescargaService;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.utils.AddEnumHTTP;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.logging.LogConstant;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.logging.UtilLog;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.microservices.core.exception.ErrorEntity;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.microservices.core.exception.InternalServerErrorException;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.microservices.core.exception.UnprocessableEntityException;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.microservices.core.util.EnumHTTP;

@RestController
@RequestMapping(value = "/nfs")
public class NFSRestService {
	
	@Value("${application.rutaBase}")
	private String rutabase;
	
	@Autowired(required = true)
	private IProcesarDescargaService procesadorDescargas;
	

	@PostMapping("/individual")
	public ResponseEntity consultaIndividualValidada(
			@RequestBody @Valid MensajeArchivoDto mensajeArchivoValidado) {
		UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "INICIO - PROCEDIMIENTO RECUPERACION ARCHIVO ENVIO INDIVIDUAL");
		MensajeRespuesta mensajeRespuesta = new MensajeRespuesta();
		try {

			if (mensajeArchivoValidado == null) {
				throw new UnprocessableEntityException(new ErrorEntity("1000", "Parametros no enviados o vacio"));
			}
			mensajeRespuesta.getResults().add(procesadorDescargas.descargarArchivo(mensajeArchivoValidado));
				} catch (Exception ex) {
					UtilLog.imprimirLog(LogConstant.LEVEL_ERROR,
							"ARCHIVO INVÁLIDO: " + mensajeArchivoValidado.getNomArchivo());
					String[] strEx = ex.getMessage().toString().split("\\|");
					mensajeRespuesta.getErrors().add(new PartialError(strEx[0],strEx[1]));
				}
			if (!mensajeRespuesta.getErrors().isEmpty()) {
				mensajeRespuesta.setCod(EnumHTTP.HTTP_ERROR_422.getCod());
				mensajeRespuesta.setMsg(EnumHTTP.HTTP_ERROR_422.getMsg());
				UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "FINAL - PROCEDIMIENTO RECUPERACION ARCHIVO ENVIO INDIVIDUAL");
				return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(mensajeRespuesta);
				
			} else {
				mensajeRespuesta.setCod(AddEnumHTTP.RESPONSE_200.getCod());
				mensajeRespuesta.setMsg(AddEnumHTTP.RESPONSE_200.getMsg());
				UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "FINAL - PROCEDIMIENTO RECUPERACION ARCHIVO ENVIO INDIVIDUAL");
				return ResponseEntity.status(HttpStatus.OK).body(mensajeRespuesta);
			}
	};
	
	@PostMapping("/listado")
	public ResponseEntity consultaListadoValidada(@RequestBody @Valid MensajeDatosDto mensajeDatosValidado) {
		UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "INICIO - PROCEDIMIENTO RECUPERACION ARCHIVO ENVIO LISTADO");

		try {
			MensajeRespuesta mensajeRespuesta = new MensajeRespuesta();
			if (mensajeDatosValidado == null) {
				throw new UnprocessableEntityException(new ErrorEntity("1000", "Parametros no enviados o vacio"));
			}

			for (MensajeArchivoDto nuevoArchivo : mensajeDatosValidado.getLstArchivos()) {
				try {
					mensajeRespuesta.getResults().add(procesadorDescargas.descargarArchivo(nuevoArchivo));
				} catch (Exception ex) {
					UtilLog.imprimirLog(LogConstant.LEVEL_ERROR,
							"ARCHIVO INVÁLIDO EN LA LISTA: " + nuevoArchivo.getNomArchivo());
					String[] strEx = ex.getMessage().split("\\|");
					mensajeRespuesta.getErrors().add(new PartialError(strEx[0],strEx[1]));
				}
			}
			if (!mensajeRespuesta.getErrors().isEmpty()) {
				mensajeRespuesta.setCod(AddEnumHTTP.RESPONSE_200.getCod());
				mensajeRespuesta.setMsg(AddEnumHTTP.RESPONSE_200.getMsg());
				UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "FINAL - PROCEDIMIENTO RECUPERACION ARCHIVO ENVIO LISTADO");
				return ResponseEntity.status(HttpStatus.OK).body(mensajeRespuesta);
			} else {
				mensajeRespuesta.setCod(AddEnumHTTP.RESPONSE_206.getCod());
				mensajeRespuesta.setMsg(AddEnumHTTP.RESPONSE_206.getMsg());
				UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "FINAL - PROCEDIMIENTO RECUPERACION ARCHIVO ENVIO LISTADO");
				return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).body(mensajeRespuesta);

			}
		} catch (Exception e) {
			UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "FINAL - PROCEDIMIENTO RECUPERACION ARCHIVO ENVIO LISTADO");
			throw new InternalServerErrorException(e);
		}
	}

}







public class ArchivoRespuesta {
	private String nomArchivo;
	private String base64;
	
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
	public ArchivoRespuesta(String nomArchivo, String base64) {
		super();
		this.nomArchivo = nomArchivo;
		this.base64 = base64;
	}
	public ArchivoRespuesta() {
		super();
	}
	
	
}






package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.bean;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

public class MensajeRespuesta {
	private String cod;
	private String msg;
	
	@JsonInclude(content = JsonInclude.Include.NON_NULL, value = JsonInclude.Include.NON_EMPTY)
	private List<ArchivoRespuesta> results;
	
	@JsonInclude(content = JsonInclude.Include.NON_NULL, value = JsonInclude.Include.NON_EMPTY)
	private List<PartialError> errors;
	
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
	public List<ArchivoRespuesta> getResults() {
		return results;
	}
	public void setResults(List<ArchivoRespuesta> results) {
		this.results = results;
	}
	public List<PartialError> getErrors() {
		return errors;
	}
	public void setErrors(List<PartialError> errors) {
		this.errors = errors;
	}
	public MensajeRespuesta(String cod, String msg, List<ArchivoRespuesta> results, List<PartialError> errors) {
		super();
		this.cod = cod;
		this.msg = msg;
		this.results = results;
		this.errors = errors;
	}
	public MensajeRespuesta() {
		super();
		List<ArchivoRespuesta> ar =  new ArrayList<>() ;
		this.results =  ar;
		List<PartialError> pe =  new ArrayList<>() ;
		this.errors = pe;
	}
	
	
}




package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.bean;

public class PartialError {
	private String codError;
	private String desError;
	
	public PartialError() {
		super();
	}
	public PartialError(String codError, String desError) {
		super();
		this.codError = codError;
		this.desError = desError;
	}	
	public String getCodError() {
		return codError;
	}
	public String getDesError() {
		return desError;
	}
	public void setCodError(String codError) {
		this.codError = codError;
	}
	public void setDesError(String desError) {
		this.desError = desError;
	}
	
}





package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.validation.ValidarNombreArchivo;

//@NotEmpty(message = "1000|Parametros no enviados o vacio")
public class MensajeArchivoDto {
	
	@NotNull(message = "1000|El campo \u201CnomArchivo\u201D no puede ser nulo.")
	@NotEmpty(message = "1001|El campo \u201CnomArchivo\u201D no puede ser vac\u00EDo.")
	@NotBlank(message = "1002|El campo \u201CnomArchivo\u201D no ha sido enviado.")
	@ValidarNombreArchivo(message = "1003| El campo \\u201CnomArchivo\\u201D no cumple con el formato correcto.")
	private String nomArchivo;
	
	@NotNull(message = "1000|El campo \u201Cruta\u201D no puede ser nulo.")
	@NotEmpty(message = "1001|El campo \u201Cruta\u201D no puede ser vac\u00EDo.")
	@NotBlank(message = "1002|El campo \u201Cruta\u201D no ha sido enviado.")
	private String ruta;
	
	public String getNomArchivo() {
		return nomArchivo;
	}
	public void setNomArchivo(String nomArchivo) {
		this.nomArchivo = nomArchivo;
	}
	public String getRuta() {
		return ruta;
	}
	public void setRuta(String ruta) {
		this.ruta = ruta;
	}
}






package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.dto;

import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class MensajeDatosDto {

	//LISTA ARCHIVOS CONSULTAR
    @NotNull(message = "1012|El campo \u201ClstArchivos\u201D no enviado.")
    @NotEmpty(message = "1013|El campo \u201ClstArchivos\u201D es vac\u00EDo.")
	@Valid
	private List<MensajeArchivoDto> lstArchivos;

	public List<MensajeArchivoDto> getLstArchivos() {
		return lstArchivos;
	}

	public void setLstArchivos(List<MensajeArchivoDto> lstArchivos) {
		this.lstArchivos = lstArchivos;
	}
    
}





package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.services;

import java.io.IOException;

import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.bean.ArchivoRespuesta;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.dto.MensajeArchivoDto;

public interface IProcesarDescargaService {
	 public ArchivoRespuesta descargarArchivo(MensajeArchivoDto msgArchivo) throws Exception;
}







package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.services.impl;

import java.io.ByteArrayOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.bean.ArchivoRespuesta;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.dto.MensajeArchivoDto;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.services.IProcesarDescargaService;
import pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.utils.CodigoAplicacion;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.logging.LogConstant;
import pe.gob.sunat.tecnologiams2.arquitectura.framework.logging.UtilLog;

@Service
public class ProcesarDescargaServicesImpl implements IProcesarDescargaService {

	@Value("${application.rutaBase}")
	private String rutabase;
	
	@Override
	public ArchivoRespuesta descargarArchivo(MensajeArchivoDto msgArchivo) throws Exception {
		UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "INICIO PROCESO INDIVIDUAL ARCHIVO "+msgArchivo.getNomArchivo());
		
		ArchivoRespuesta archivoRespuesta = new ArchivoRespuesta();
			Path rutaCompleta = Paths.get(rutabase+msgArchivo.getRuta(),  msgArchivo.getNomArchivo());
			File file = rutaCompleta.toFile();
			
			archivoRespuesta.setNomArchivo(msgArchivo.getNomArchivo());
			
			if (!file.exists()) {	
				UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "NO SE ENCONTRO EL ARCHIVO "+msgArchivo.getNomArchivo()+" EN LA RUTA ESPECIFICADA.");
				throw new Exception(CodigoAplicacion.APP_1006.getMsg());
			}
		
			try (FileInputStream fileInputStream = new FileInputStream(file);
					ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
				byte[] buffer = new byte[1024];
				int length;
				while ((length = fileInputStream.read(buffer)) != -1) {
					byteArrayOutputStream.write(buffer, 0, length);
				}

				byte[] bytes = byteArrayOutputStream.toByteArray();
				archivoRespuesta.setBase64(Base64.getEncoder().encodeToString(bytes));
				return archivoRespuesta;
			} catch (Exception e) {
				
				UtilLog.imprimirLog(LogConstant.LEVEL_INFO, "ERROR DURANTE LA DESCARGA DEL ARCHIVO "+msgArchivo.getNomArchivo()+" DESDE LA RUTA ESPECIFICADA.");
				throw new Exception(CodigoAplicacion.APP_1007.getMsg());
			}
	
	}
}






package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.utils;

public enum AddEnumHTTP  {
    RESPONSE_200("200", "La solicitud ha sido completada con \u00E9xito."),
    RESPONSE_201("201", "La solicitud ha sido completada y se ha creado un nuevo recurso."),
    RESPONSE_204("204", "La solicitud se ha completado con \u00E9xito pero no hay contenido para devolver."),
    RESPONSE_206("206", "La solicitud se ha completado con \u00E9xito, pero algunos archivos no se procesaron completamente."),
    RESPONSE_406("406", "El servidor no puede generar una respuesta que sea aceptable para el cliente."),
    RESPONSE_409("409", "La solicitud no se pudo completar debido a un conflicto con el estado actual del recurso."),
    RESPONSE_415("415", "El servidor no admite el tipo de medio de la entidad solicitada."),
    RESPONSE_507("507", "No se puede completar la acci\u00F3n debido a problemas de almacenamiento en el servidor");
    

 
    private final String cod;
    private final String msg;
    
    AddEnumHTTP(String cod, String msg) {
        this.cod = cod;
        this.msg = msg;
    }

	public String getCod() {
		return cod;
	}

	public String getMsg() {
		return msg;
	}
 

 
}





package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.utils;

public enum CodigoAplicacion {
	APP_1000("1000", "1000|El campo \u201C{0}\u201D no puede ser nulo."),
	APP_1001("1001", "1001|El campo \u201C{0}\u201D no ha sido enviado."),
	APP_1002("1002", "1002|El campo \u201C{0}\u201D no puede ser vac\u00EDo."),
	APP_1003("1003", "1003|El campo \u201C{0}\u201D no cumple con el formato correcto."),
	APP_1004("1004", "1004|El archivo %s se guardó correctamente."),
	APP_1005("1005", "1005|El archivo %s no se logró registrar."),
	APP_1006("1006", "1006|No se encontro la ruta especificada."),
	APP_1007("1007", "1007|Error no controlado durante la descarga del archivo");
    private final String cod;
    private final String msg;
    
    CodigoAplicacion(String cod, String msg) {
        this.cod = cod;
        this.msg = msg;
    }

	public String getCod() {
		return cod;
	}

	public String getMsg() {
		return msg;
	}
 
}





package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.utils;

import java.text.SimpleDateFormat;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonInclude.Value;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JsonUtil {

    private static final ObjectMapper mapper;
    private static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setDefaultPropertyInclusion(Value.construct(Include.NON_NULL, Include.ALWAYS));
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.registerModule(new JavaTimeModule());
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
        mapper.setDateFormat(sdf);
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

}
	
	
	
	
package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConstraintNombreArchivo implements ConstraintValidator<ValidarNombreArchivo, String> {
	
	@Override
	public boolean isValid(String value, ConstraintValidatorContext context) {		
		if (value != null) {
			boolean res = false;
			if (esNumerico(value.split("-")[1])) {
				// INDIVIDUALES
				String regex = "\\d{11}-\\d{2}-[A-Z0-9]{4}-\\d{1,8}.[zip,ZIP]{3}";
				res = validarCadena(value, regex);
			} else {
				// RESUMENES
				String regex = "\\d{11}-[A-Z]{2}-[0-9]{8}-\\d{1,8}.[zip,ZIP]{3}";						
				res = validarCadena(value, regex);
			}
			return res;
		}
		return true;
	}

	public static boolean esNumerico(String str) {
		return str.matches("\\d+");
	}

	public static boolean validarCadena(String cadena, String regex) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(cadena);
		return matcher.matches();
	}

}




package pe.gob.sunat.contribuyentems2.registro.cpe.consulta.nfs.backend.validation;


import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = {ConstraintNombreArchivo.class})
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidarNombreArchivo {
    String message() default "";


    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
