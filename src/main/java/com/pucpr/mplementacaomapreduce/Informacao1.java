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


public class Informacao1 {
    public static class MapperInformacao1 extends Mapper<Object,Text,Text, IntWritable >{
        public void map(Object chave,Text valor,Context context) throws IOException, InterruptedException{
            //1. País com a maior quantidade de transações comerciais efetuadas.
            //pais;ano;codigo;mercadoria;fluxo;valor;peso;unidade;quantidade;categoria;
            String linha = valor.toString();
            String [] campos=linha.split(";");
            if (campos.length==10){
                String pais= campos[0];
                int ocorrencia= 1;
                Text chaveMap = new Text(pais);
                IntWritable valorMap= new IntWritable(ocorrencia);
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerInformacao1 extends Reducer<Text,IntWritable,Text,IntWritable>{
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
        String arquivoSaida="/home2/ead2021/SEM1/adilson.reis/Desktop/implementacaoLocalMR/informacao1";
        if(args.length==2){
            arquivoEntrada=args[0];
            arquivoSaida=args[1];
            
        }
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "atividade2");
        job.setJarByClass(Informacao1.class);
        job.setMapperClass(MapperInformacao1.class);
        job.setReducerClass(ReducerInformacao1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path (arquivoSaida));
        job.waitForCompletion(true);
    }
    
}
