package com.example.bigdata.tools;

import org.apache.flink.api.java.utils.ParameterTool;

public class Properties {
    public static ParameterTool get(String[] args) {

        ParameterTool propertiesFromArgs = ParameterTool.fromArgs(args);

        try {
            // Próba załadowania pliku z parametrami
            ParameterTool propertiesFromFile = ParameterTool.fromPropertiesFile("flink.properties");
            return propertiesFromFile.mergeWith(propertiesFromArgs);
        } catch (Exception e) {
            // Jeśli plik nie istnieje, użyj tylko argumentów
            return propertiesFromArgs;
        }
    }
}
