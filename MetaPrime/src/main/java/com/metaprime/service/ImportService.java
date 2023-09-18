package com.metaprime.service;


import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ImportService {
    //Данные для подключения к БД. Использовался PostgreSQL
    private static String URL = "jdbc:postgresql://localhost:5432/MetaPrime";
    private static String USERNAME = "postgres";
    private static String PASSWORD = "Sova1029";
    /**
     * Основной метод сервиса, который парсит XML файл,
     * используя библиотеку StAX и с помощью PreparedStatement
     * добавляются уникальные данные в БД
     */
    public static void main(String[] args){
        //Основной запрос
        String query = "INSERT INTO Orders (article, name, price)" +
                "VALUES (?,?,?) ON CONFLICT ON CONSTRAINT unique_combination DO NOTHING;";
        //Подключение в try with resources
        try (PreparedStatement preparedStatement = DriverManager.
                getConnection(URL,USERNAME,PASSWORD).prepareStatement(query)){
            //StAX парсер
            String path =  "src/main/resources/Orders.xml";
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            XMLStreamReader xmlStreamReader = xmlInputFactory.createXMLStreamReader(new FileInputStream(path));
            // Переменные для хранения данных из XML
            int article = 0;
            String name = null;
            int price = 0;
            //Парсинг XML файла
            while(xmlStreamReader.hasNext()){
                int event = xmlStreamReader.next();
                //Если начинается событие, то происходит получение данных из файла
                if (event == XMLStreamConstants.START_ELEMENT){
                    switch (xmlStreamReader.getLocalName()){
                        case "article"      -> article = Integer.parseInt(xmlStreamReader.getElementText());
                        case "name"         -> name = xmlStreamReader.getElementText();
                        case "price"        -> price = Integer.parseInt(xmlStreamReader.getElementText());
                    }
                    //Если заканчивается событие, то происходит добавление данных в запрос
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    if (xmlStreamReader.getLocalName().equals("Order")) {
                        preparedStatement.setInt(1, article);
                        preparedStatement.setString(2, name);
                        preparedStatement.setInt(3, price);
                        preparedStatement.addBatch();
                        article = 0;
                        name = null;
                        price = 0;
                    }
                }
            }
            //Выполнение вставки
            int[] batchResult = preparedStatement.executeBatch();
            System.out.println("Successful addition of data");
            //Обработка исключений, выводя информацию в консоль
        } catch(IOException e){
            System.out.println("Exception: "+ e.getMessage());
        } catch(SQLException e){
            System.out.println("Exception: "+ e.getMessage());
        } catch (XMLStreamException e) {
            System.out.println("Exception: "+ e.getMessage());
        }
    }

}
