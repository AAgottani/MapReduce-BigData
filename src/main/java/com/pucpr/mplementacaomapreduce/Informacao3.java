package com.pucpr.mplementacaomapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Informacao3 {
    public static class MapperInformacao3 extends Mapper<Object,Text,Text, IntWritable >{
        public void map(Object chave,Text valor,Context context) throws IOException, InterruptedException{
            //3.Quantidade de transações comerciais realizadas por ano
            //pais;ano;codigo;mercadoria;fluxo;valor;peso;unidade;quantidade;categoria;
            String linha = valor.toString();//recebe a linha do documento csv
            String [] campos=linha.split(";");//divide a linha em campos
            if (campos.length==10){//confere se a quantidade de campos está completa em cada linha
                String ano= campos[1];//recebe um string com o campo selecionado no indice
                int ocorrencia= 1; //recebe um inteiro no valor de 1
                Text chaveMap = new Text(ano);//transforma  a string em text recebendo como parametro o campo desejado
                IntWritable valorMap= new IntWritable(ocorrencia);//transforma inteiro em intw parametro ocorrencia
                context.write(chaveMap, valorMap);//escreve a chave e o valor 
            }
        }
    }
    
    public static class ReducerInformacao3 extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text chave,Iterable<IntWritable> valores,Context context) throws IOException, InterruptedException{
            int soma=0;
            for(IntWritable val:valores){
                soma+=val.get();
            }
            IntWritable valorSaida= new IntWritable(soma);
            context.write(chave, valorSaida);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada="/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida="/home2/ead2021/SEM1/adilson.reis/Desktop/implementacaoLocalMR/informacao3";
        if(args.length==2){
            arquivoEntrada=args[0];
            arquivoSaida=args[1];
            
        }
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "atividade4");
        job.setJarByClass(Informacao3.class);
        job.setMapperClass(MapperInformacao3.class);
        job.setReducerClass(ReducerInformacao3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path (arquivoSaida));
        job.waitForCompletion(true);
    }
    
}
