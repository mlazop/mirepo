public String buscarArchivoNFS(String textoJson) {
        if (log.isDebugEnabled()) log.debug("Inicio - consultarNFS textoJson: " + textoJson);
        String resp = "";

        HttpURLConnection con = null;
        BufferedReader br = null;
        OutputStream os = null;
        try {
            // Obtener la URL del servicio y verificar si está habilitado
            T01Bean pbean = daoT01.findByNumeroArgumento("967", "INTRES16152601");
            String s_uri = pbean.getFuncion().substring(0, 119).trim();
            String s_estadoWS = pbean.getFuncion().substring(124, 125).trim();
            if ("0".equals(s_estadoWS)) {
                throw new RuntimeException("Servicio Web se encuentra inhabilitado. WS: INTRES16152601");
            }
            if (log.isDebugEnabled()) log.debug("pasando el servicio  " + s_uri);
            URL urlObj = new URL(s_uri);
            con = (HttpURLConnection) urlObj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            // Envío del JSON utilizando JsonUtil para excluir propiedades null
            ArchivoNfsResponseDTO requestDto = new ArchivoNfsResponseDTO();
            requestDto.setCod("001");
            requestDto.setMsg(null); // Este campo será excluido
            // Añadir más campos al requestDto según sea necesario

            JsonConfig jsonConfig = JsonUtil.getJsonNoNull();
            JSONObject jsonObject = JSONObject.fromObject(requestDto, jsonConfig);
            String jsonInputString = jsonObject.toString();

            os = con.getOutputStream();
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);

            // Lectura de la respuesta
            int status = con.getResponseCode();
            if (status == HttpURLConnection.HTTP_OK) {
                br = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuilder content = new StringBuilder();
                while ((inputLine = br.readLine()) != null) {
                    content.append(inputLine);
                }
                // Deserializar el JSON a un objeto Java
                ObjectMapper mapper = new ObjectMapper();
                ArchivoNfsResponseDTO response = mapper.readValue(content.toString(), ArchivoNfsResponseDTO.class);
                // Usar los datos del objeto Java
                byte[] decodedBytes = null;
                String decodedString = null;
                if (response.getResults().length > 0) {
                    ArchivoNfsResponseDTO.Result result = response.getResults()[0];
                    if (log.isDebugEnabled()) log.debug("NomArchivo::::::>" + result.getNomArchivo());
                    if (log.isDebugEnabled()) log.debug("ValArchivo::::::> " + result.getBase64());
                    String base64String = result.getBase64();
                    decodedBytes = decompress(decodedBytes);
                    decodedString = new String(decodedBytes, "UTF-8");
                } else {
                    if (log.isDebugEnabled()) log.debug("No hay resultados disponibles.");
                }
                if (log.isDebugEnabled()) log.debug("ResultObject: ");
                return decodedString;
            } else {
                if (log.isDebugEnabled()) log.debug("Error: " + status);
                return null;
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("consultarNFS - Except: " + e);
            return null;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception e) {
                if (log.isDebugEnabled()) log.debug("consultarNFS - Except br: " + e);
            }
            if (con != null) {
                con.disconnect();
            }
        }
    }
	
	
	
	
	
		String textoJson="";
		String nomArchivo="";
		if(log.isDebugEnabled()) log.debug("fechaEmi: "+fechaEmi);
		if (fechaEmi != null) {
			Calendar calendario = Calendar.getInstance();
			calendario.setTime(fechaEmi);
			if(log.isDebugEnabled()) log.debug("obtenerJson paso2");
			String annio = String.valueOf(calendario.get(Calendar.YEAR));
			String mes = String.format("%02d", calendario.get(Calendar.MONTH) + 1);
			String dia = String.format("%02d", calendario.get(Calendar.DAY_OF_MONTH));
			String hora = String.format("%02d", calendario.get(Calendar.HOUR_OF_DAY));
			if(log.isDebugEnabled()) log.debug("annio: "+annio+" - mes: "+mes+" - dia: "+dia+" - hora: "+hora);
			char ultimoDigitoRuc = pRuc.charAt(pRuc.length() - 1);
			//String ruta = "No encontro ruta";
			int inicio = Math.max(0, pRuc.length() - 5); 
			String cuatroDigitosAnterioresRuc = pRuc.substring(inicio, pRuc.length() - 1);
			if (log.isDebugEnabled()) log.debug("cuatroDigitosAnterioresRuc: " + cuatroDigitosAnterioresRuc);
			//String ruta= annio +"/"+mes+"/"+dia+"/"+hora+"/"+ultimoDigitoRuc+"/"+codCpe+"/0";
			String ruta= annio +"/"+mes+"/"+dia+"/"+ultimoDigitoRuc+"/"+cuatroDigitosAnterioresRuc+"/"+pRuc+"/"+xtipo+"/1";
			if (log.isDebugEnabled()) log.debug("mostrar ruta: " + ruta);
			nomArchivo=pRuc + "-"+ xtipo + "-" + pSerie + "-" + pNumero + ".zip";
			textoJson = "{\"nomArchivo\":\"" + nomArchivo + "\",\"ruta\":\""+ruta+"\"}";
		} else {
			if (log.isDebugEnabled()) log.debug("Fecha de emisión no encontrada para numRuc: " + pRuc + ", serie: " + pSerie +
                    ", numCpe: " + pNumero + ", codCpe: " + xtipo);
			textoJson=null;
		}
		
		
		
		
		

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


