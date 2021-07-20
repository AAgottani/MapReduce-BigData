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


public class informacao5 {
    public static class MapperImplementacao5 extends Mapper<Object,Text,Text, IntWritable >{
        public void map(Object chave,Text valor,Context context) throws IOException, InterruptedException{
            //4.mercadoria com a maior quantidade de transações finaceiras em 2016
            //pais;ano;codigo;mercadoria;fluxo;valor;peso;unidade;quantidade;categoria;
            String linha = valor.toString();
            String [] campos=linha.split(";");
            if (campos.length==10 && campos[1].equals("2016")){
                String mercadoria= campos[3];
                int ocorrencia= 1;
                Text chaveMap = new Text(mercadoria);
                IntWritable valorMap= new IntWritable(ocorrencia);
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerImplementacao5 extends Reducer<Text,IntWritable,Text,IntWritable>{
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
        String arquivoSaida="/home2/ead2021/SEM1/adilson.reis/Desktop/implementacaoLocalMR/informacao5";
        if(args.length==2){
            arquivoEntrada=args[0];
            arquivoSaida=args[1];
            
        }
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "atividade5");
        job.setJarByClass(informacao5.class);
        job.setMapperClass(MapperImplementacao5.class);
        job.setReducerClass(ReducerImplementacao5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path (arquivoSaida));
        job.waitForCompletion(true);
    }
    
}
