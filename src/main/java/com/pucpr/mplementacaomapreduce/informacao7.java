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


public class informacao7 {
    public static class MapperImplementacao7 extends Mapper<Object,Text,Text, IntWritable >{
        public void map(Object chave,Text valor,Context context) throws IOException, InterruptedException{
            //mercadoria com maior total de peso, de acordo com todas as transações comerciais 
            //pais;ano;codigo;mercadoria;fluxo;valor;peso;unidade;quantidade;categoria;
            
            String linha = valor.toString();
            String [] campos=linha.split(";");
            
            IntWritable valorMap= new IntWritable(0);
            
            if (campos.length==10){
                String merc= campos[3];
                String peso= campos[6];
                
                Text chaveMap = new Text(merc);
                try{
                    valorMap= new IntWritable(Integer.parseInt(peso));
                
                }catch(NumberFormatException e){
                    
                }finally{
                }
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerImplementacao7 extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text chave,Iterable<IntWritable> valores,Context context) throws IOException, InterruptedException{
            long soma=0;
            
            IntWritable valorSaida= new IntWritable(0);
            for(IntWritable val:valores){
                soma+=val.get();
            }try{
            valorSaida.set(Integer.parseInt(String.valueOf(soma)));
            }catch (NumberFormatException e){
            }finally{
            }
            
            context.write(chave, valorSaida);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada="/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida="/home2/ead2021/SEM1/adilson.reis/Desktop/implementacaoLocalMR/informacao7";
        if(args.length==2){
            arquivoEntrada=args[0];
            arquivoSaida=args[1];
            
        }
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "atividade5");
        job.setJarByClass(informacao7.class);
        job.setMapperClass(MapperImplementacao7.class);
        job.setReducerClass(ReducerImplementacao7.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path (arquivoSaida));
        job.waitForCompletion(true);
    }
    
}
