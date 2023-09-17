package com.metaprime.service;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
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
     * используя библиотеку DOM Parser и с помощью PreparedStatement
     * добавляются уникальные данные в БД
     */
    public static void main(String[] args){
        try{
            //Парсинг XML файла
            String path =  "src/main/resources/Orders.xml";
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(new File(path));
            //Создание подключения
            Connection connection = DriverManager.getConnection(URL,USERNAME,PASSWORD);
            //Создание запроса
            NodeList orderList = document.getElementsByTagName("Order");
            String query = "INSERT INTO Orders (article, name, price)" +
                    "VALUES (?,?,?) ON CONFLICT ON CONSTRAINT unique_combination DO NOTHING;";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            for (int i = 0; i<orderList.getLength();i++){
                Element el = (Element) orderList.item(i);
                int article = Integer.parseInt(el.getElementsByTagName("article").item(0).getTextContent());
                String name = el.getElementsByTagName("name").item(0).getTextContent();
                int price = Integer.parseInt(el.getElementsByTagName("price").item(0).getTextContent());

                preparedStatement.setInt(1,article);
                preparedStatement.setString(2,name);
                preparedStatement.setInt(3,price);
                preparedStatement.addBatch();
            }
            //Выполнение вставки
            int[] batchResult = preparedStatement.executeBatch();
            //Закрытие ресурсов
            preparedStatement.close();
            connection.close();
            //Обработка исключений, выводя информацию в консоль
        } catch(ParserConfigurationException e) {
            System.out.println("Exception: "+ e.getMessage());
        } catch(IOException e){
            System.out.println("Exception: "+ e.getMessage());
        } catch(SAXException e){
            System.out.println("Exception: "+ e.getMessage());
        } catch(SQLException e){
            System.out.println("Exception: "+ e.getMessage());
        }
    }

}
