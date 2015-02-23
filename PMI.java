import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class PMI 
{

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		//Problem 3
		String path="./test/";
		ArrayList<String> files=new ArrayList<String>();
		files.add(path+"part-r-00000");
		//files.add(path+"part-r-00001");
		//files.add(path+"part-r-00002");
		//files.add(path+"part-r-00003");
		//files.add(path+"part-r-00004");
		int filecnt=files.size();
		String filename="";
		Map<String,Double> pairmap=new HashMap<String,Double>();
		double maxPMI=-100000;
		String maxPair=null;
		String[] cloudmaxwords={"","","","","",""};
		String[] lovemaxwords={"","","","","",""};
		Double[] cloudmaxpmi={-100000.0,-100000.0,-100000.0,-100000.0,-100000.0,-100000.0};
		Double[] lovemaxpmi={-100000.0,-100000.0,-100000.0,-100000.0,-100000.0,-100000.0};
		String word="";
		try
		{
			for (int i=0;i<filecnt;i++)
			{
				filename=files.get(i);
				InputStreamReader reader = new InputStreamReader(new FileInputStream(filename)); // 建立一个输入流对象reader  
		        BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言  
		        String line = null;   
		        while ((line = br.readLine()) != null)
		        {
		            String[] words=line.split("\\s+");
		            System.out.println(words[0]+":"+words[1]+":"+words[2]);
		            /*if (pairmap.containsKey(words[0]+words[1]))
		            {
		            	System.out.println("duplicate pairs:"+words[0]+words[1]);
		            }*/
		            //for p3
		            double pmi=Double.parseDouble(words[2]);
		            //for p4
		            pairmap.put(words[0]+words[1], pmi);
		            if (pmi>maxPMI)
		            {
		            	maxPMI=pmi;
		            	maxPair=words[0]+words[1];
		            }
		            //for p5
		            String word0=words[0].substring(1, words[0].length()-1);
		            String word1=words[1].substring(0, words[1].length()-1);
		            if (word0.equalsIgnoreCase("love")||word1.equalsIgnoreCase("love"))
		            {
		            	if (pmi>lovemaxpmi[0])
		            	{
		            		lovemaxpmi[0]=pmi;
		            		lovemaxwords[0]=word0+","+word1;
		            	}
		            	else if (pmi>lovemaxpmi[1])
		            	{
		            		lovemaxpmi[1]=pmi;
		            		lovemaxwords[1]=word0+","+word1;
		            	}
		            	else if (pmi>lovemaxpmi[2])
		            	{
		            		lovemaxpmi[2]=pmi;
		            		lovemaxwords[2]=word0+","+word1;
		            	}
		            	else if (pmi>lovemaxpmi[3])
		            	{
		            		lovemaxpmi[3]=pmi;
		            		lovemaxwords[3]=word0+","+word1;
		            	}
		            	else if (pmi>lovemaxpmi[4])
		            	{
		            		lovemaxpmi[4]=pmi;
		            		lovemaxwords[4]=word0+","+word1;
		            	}
		            	else if (pmi>lovemaxpmi[5])
		            	{
		            		lovemaxpmi[5]=pmi;
		            		lovemaxwords[5]=word0+","+word1;
		            	}
		            }
		            
		            if (word0.equalsIgnoreCase("cloud")||word1.equalsIgnoreCase("cloud"))
		            {
		            	if (pmi>cloudmaxpmi[0])
		            	{
		            		cloudmaxpmi[0]=pmi;
		            		cloudmaxwords[0]=word0+","+word1;
		            	}
		            	else if (pmi>cloudmaxpmi[1])
		            	{
		            		cloudmaxpmi[1]=pmi;
		            		cloudmaxwords[1]=word0+","+word1;
		            	}
		            	else if (pmi>cloudmaxpmi[2])
		            	{
		            		cloudmaxpmi[2]=pmi;
		            		cloudmaxwords[2]=word0+","+word1;
		            	}
		            	else if (pmi>cloudmaxpmi[3])
		            	{
		            		cloudmaxpmi[3]=pmi;
		            		cloudmaxwords[3]=word0+","+word1;
		            	}
		            	else if (pmi>cloudmaxpmi[4])
		            	{
		            		cloudmaxpmi[4]=pmi;
		            		cloudmaxwords[4]=word0+","+word1;
		            	}
		            	else if (pmi>cloudmaxpmi[5])
		            	{
		            		cloudmaxpmi[5]=pmi;
		            		cloudmaxwords[5]=word0+","+word1;
		            	}
		            }
		        }  
		        br.close();
			
			}
		}
		catch(IOException e)
		{
			System.out.println(e.toString());
		}
		//for problem 3:
		System.out.println("for problem 3: "+pairmap.size());
		//for problem 4:
		System.out.println("for problem 4: "+maxPair+" : "+maxPMI);
		//for problem 5:
		System.out.println("for problem 5: the max words relates to love is: --1: " +lovemaxwords[0]+" : "+lovemaxpmi[0]+" --2: "+lovemaxwords[2]+" : "+lovemaxpmi[2]+" --3:"+lovemaxwords[4]+" : "+lovemaxpmi[4]);
		System.out.println("for problem 5: the max words relates to cloud is: --1: " +cloudmaxwords[0]+" : "+cloudmaxpmi[0]+" --2: "+cloudmaxwords[2]+" : "+cloudmaxpmi[2]+" --3:"+cloudmaxwords[4]+" : "+cloudmaxpmi[4]);
	}
}
