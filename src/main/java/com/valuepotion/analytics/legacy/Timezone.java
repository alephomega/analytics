package com.valuepotion.analytics.legacy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.valuepotion.analytics.core.AnalyticsDriver;
import com.valuepotion.analytics.core.AnalyticsMapper;
import com.valuepotion.analytics.core.AnalyticsReducer;
import com.valuepotion.analytics.core.CombineKeyValueTextInputFormat;
import com.valuepotion.analytics.core.LineDataTool;

public class Timezone extends AnalyticsDriver {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Timezone(), args);
	}

	@Override
	public Job prepareJob(Configuration conf, String input, String output) throws IOException {

		return prepareJob(conf, 
				"valuepotion.analytics.timezone", 
				input,
				output, 
				CombineKeyValueTextInputFormat.class,
				TimezoneMapper.class, 
				Text.class, 
				Text.class, 
				TimezoneReducer.class,
				Text.class, 
				Text.class, 
				null, 
				null, 
				null, 
				null,
				TextOutputFormat.class);
	}

	static class TimezoneMapper extends AnalyticsMapper<Text, Text, Text, Text> {
		private Map<String, String> timezoneMap = new HashMap<String, String>();
		private Text k = new Text();

		@Override
		protected void setup(Context context) {
			timezoneMap.put("2e46ad420b630ca84bebe33676750cc9", "A0900");
			timezoneMap.put("d516ce2cffe98ccf7577343fd654e3cf", "A0900");
			timezoneMap.put("7c356163521846cea82b3e0927eeacaf", "A0900");
			timezoneMap.put("839d5b4e780b81fdd5282a1a480adb6b", "A0900");
			timezoneMap.put("c69666780f41b270686a8664772aeb41", "A0900");
			timezoneMap.put("ac87bf0e0f74b0100542ae6e1dda11c3", "A0900");
			timezoneMap.put("bcc6fc187604181d03655525e2ef6875", "A0900");
			timezoneMap.put("92798ab0c298d31ddf1493b49e69ab76", "A0900");
			timezoneMap.put("f1247aba87e5fa3c88143604c0836a3b", "A0900");
			timezoneMap.put("2b587343100220c434e8a0a2cb9bb797", "A0900");
			timezoneMap.put("c7761526c38038332d533a299944fdf1", "A0900");
			timezoneMap.put("81d8103f2eff642e90a9513295ae5bd3", "A0900");
			timezoneMap.put("216cbc2184a7ff45cb42bc24e86f0293", "A0900");
			timezoneMap.put("a5a810ab61b00b6ca4ab42eeed61e990", "A0900");
			timezoneMap.put("200bc05670108dba6db349090fd1dccb", "A0900");
			timezoneMap.put("ce1e69bff29eb3f6487f5facfac6b72c", "A0900");
			timezoneMap.put("4067acba188f43d0d2416ad6d73be53c", "A0900");
			timezoneMap.put("9b7af90371a5388951cddd8de0f8fcfb", "A0900");
			timezoneMap.put("59ebd79d32bb67ed1c3017c0b0ba556e", "A0900");
			timezoneMap.put("f6f40cdc3e9b0e4f1db28504b73ce397", "A0900");
			timezoneMap.put("7c56f8101316e58a8716bd049937c224", "A0900");
			timezoneMap.put("c7091b83778530ab099b8728f2bf9732", "A0800");
			timezoneMap.put("7555297adf1fb4b3ba31294e5b027fd1", "A0900");
			timezoneMap.put("7f647f9fe07c2692f20ed30b0341d350", "B0800");
			timezoneMap.put("ce7633ca69b05f5465e61d5d0400df62", "A0900");
			timezoneMap.put("ac0ca9ef1c595b20405634fde584f0a8", "A0900");
			timezoneMap.put("d1cd360d834ef50cfa54c22d9e545847", "B0700");
			timezoneMap.put("5fbf9c61cc6611c431330e7eb5537561", "A0900");
			timezoneMap.put("b25f178161ad3a1f952b16c29ec10bd1", "A0900");
			timezoneMap.put("d41726f64e7a35e4ddd933d8f3989095", "A0900");
			timezoneMap.put("52b1f0158b12588e3b5037deda5430fd", "A0800");
			timezoneMap.put("a030cf4995ffd73e89f44446de3fb7c4", "A0900");
			timezoneMap.put("f50e3eeeb55d1cf616c51e0b76e8a550", "A0900");
			timezoneMap.put("d298b6a0b8ca5f0a401e166f0e712cc7", "A0900");
			timezoneMap.put("51fba980e960e87fbfed6998932084ec", "A0800");
			timezoneMap.put("13231ac2ad5a4bfaa49d7eb592a8456c", "A0900");
			timezoneMap.put("1955f700ad21d4b98afd07d0e97f9a6a", "A0900");
			timezoneMap.put("38044a556b95d1a85e4f9b85f7030b86", "A0900");
			timezoneMap.put("2062781953d30a9beaebd72125451705", "A0900");
			timezoneMap.put("8b5b28e8666f445a747f80f9c4d67a9d", "A0900");
			timezoneMap.put("420f1318e091417db417dbdbdb68d913", "A0800");
			timezoneMap.put("7123d802853ea3ea2ce361f5c9f8ac9a", "A0800");
			timezoneMap.put("597208352ccc686d16cab9ea25d42ed8", "A0900");
			timezoneMap.put("44746d7133ebb5b3ef3adaa0561ad9e4", "A0900");
			timezoneMap.put("99edc99bb61d2a63e7b0481f9acde76f", "A0900");
			timezoneMap.put("da6b514fb6f85ed7da808c886580616f", "A0900");
			timezoneMap.put("1507cef5e69ca0fb1dbbadc3bc1e347d", "A0900");
			timezoneMap.put("7c6d13846699f8ea8411b85b34be7caa", "A0900");
			timezoneMap.put("ecd3172f9bf7a49a31e7307d8ebe8938", "A0900");
			timezoneMap.put("ab604de5212efa84a58f131a6e3124fc", "A0800");
			timezoneMap.put("15dcebbfb8137e91bf030e564c249e66", "B0900");
			timezoneMap.put("3f6fc3f41bb1f73182dbb6a75a0f10b8", "A0900");
			timezoneMap.put("364baff93fd0646552e15d740fc96be0", "A0900");
			timezoneMap.put("f80e02be170ef379e8aeffc9718e2e58", "A0900");
			timezoneMap.put("098a901fc9f8d4aec34f8e8d3d626528", "A0900");
			timezoneMap.put("09bc5bb20828bb601bf482b803230f06", "A0900");
			timezoneMap.put("8ca5b3c5b92ba5a3ef4e9b14b44b98b5", "A0900");
			timezoneMap.put("87f7d763ac08a820416b7467d8fcea23", "A0700");
			timezoneMap.put("3f313ea0269babf7e1ab02ebeed98dbe", "A0700");
			timezoneMap.put("7da4645bc684c2ab5b8cf0fc70fe44c6", "A0900");
			timezoneMap.put("9ae97007a853a6d1fb337abb8a7356ce", "A0900");
			timezoneMap.put("490ec8a277913fefa6104f3a9bd86941", "A0900");
			timezoneMap.put("884ec7b62f21466b65a5ccc0694dbebe", "A0800");
			timezoneMap.put("71bc5edc5e282a0aea0cb60b03386b23", "A0800");
			timezoneMap.put("e8ac129fb85fcdb74f4881df3be1b1d2", "A0900");
			timezoneMap.put("b65edc2a108f6a9743a71a3ef3b8e89a", "A0900");
			timezoneMap.put("c6fc25e69cd7469d0818127b14f03163", "A0900");
			timezoneMap.put("6a0e560dd9764e23c3230013ef89a6cf", "A0900");
			timezoneMap.put("66bb21a9cea56f1e7a8b9605a7bf1313", "A0900");
			timezoneMap.put("f5c84dfeb827c1ee9452fc5ff932bb56", "A0800");
			timezoneMap.put("a247963898d3d8634591307e073595ff", "A0900");
			timezoneMap.put("e6010069704dc887c8bdc88e9fe9ac4e", "A0900");
			timezoneMap.put("c2d0e780eab8271ccfaae5ab788e5ea7", "A0900");
			timezoneMap.put("ba83f1af1d2cda544e89efb0b91f360b", "A0530");
			timezoneMap.put("ced0ed37dfb64eeba694154dc9d8c8a4", "A0900");
			timezoneMap.put("412de48feca0a23d577681f7eb73d81f", "A0900");
			timezoneMap.put("fee1a222a4618102d9c1193b64b3ec38", "A0900");
			timezoneMap.put("2875342531fc744c23c81cfdb27e7ea5", "A0900");
			timezoneMap.put("2ade591cd6d184ff475de75c51597942", "A0900");
			timezoneMap.put("589492c0de8d661283c32921c29516de", "A0900");
			timezoneMap.put("8c924e84fa5b42b983f4aaca3f75ad82", "A0900");
			timezoneMap.put("5e8a88166c91415e914431262567053d", "A0900");
			timezoneMap.put("267b564ccc773784d89d9d3379091fed", "A0900");
			timezoneMap.put("07ac4b30ca0ce51feaf93a411d14683d", "A0900");
			timezoneMap.put("c008b771ea1a1c4d044e09e05b6bcf41", "A0900");
			timezoneMap.put("6e284e64f29e6011750c13f25e091a07", "A0900");
			timezoneMap.put("fd3b5ebcec4dd5732cec3540390bf0ee", "A0900");
			timezoneMap.put("cd245553c061e88806bd8eb0f623dc2c", "A0900");
			timezoneMap.put("44365889c8780080bb7716cd36800d33", "A0900");
			timezoneMap.put("8fa2009de63eed75e0623942a6407fff", "A0900");
			timezoneMap.put("9c8aea9ddbabe57346c8eb47276080d2", "A0900");
			timezoneMap.put("6ef18448ed7820ecb930f2bdb41c9eae", "A0900");
			timezoneMap.put("14ee31e2defdab47b54c352f4ef39ff4", "A0900");
			timezoneMap.put("138ec7bebb732a60dac8a7fe452a02a9", "A0900");
			timezoneMap.put("f0e15af49e8fc3ef804aaf68e4c9c8d3", "A0900");
			timezoneMap.put("0d8bb568a017b5ccfd66f4ae5fc16518", "A0700");
			timezoneMap.put("8c7e885ced4a86e5465b4d0e11a2dd8b", "A0900");
			timezoneMap.put("089af66b7416fdedcf4edd7206ac1644", "A0800");
			timezoneMap.put("fe917bdce0c302a3715287fc1130799f", "A0900");
			timezoneMap.put("22498efce05be22bb55660bbbc493dc7", "A0900");
			timezoneMap.put("be495636e9d3f98e221bdc363047febf", "A0900");
			timezoneMap.put("0dc37fed4759cb8a4c3aaa37b73908d9", "B0600");
			timezoneMap.put("9167cabe0173437a25a8a183eee293ec", "A0900");
			timezoneMap.put("e6c304e5f5a648513f896b4a58721acc", "A0900");
			timezoneMap.put("f39036b76b2e6cd48c47ec6820be404b", "A0900");
			timezoneMap.put("8026b1ae80b3bcb933cc388c5251954d", "B0600");
			timezoneMap.put("fdc705ba1c498dbd30925bd997d8baef", "A0900");
			timezoneMap.put("d14e818d1f3e3eaa111b123f267ac17a", "A0900");
			timezoneMap.put("8b293d0cb9498f662e183afc819a639a", "A0900");
			timezoneMap.put("5e04a41f93826b294f042c00d97f5bc3", "A0900");
			timezoneMap.put("342bdd4111c1b4c52907ab6406d39905", "A0900");
			timezoneMap.put("66fcaf87f56e65c65abd7f56cd7579d9", "A0800");
			timezoneMap.put("f8001b98a1495226863597cbf9308875", "A0900");
			timezoneMap.put("1e9b79c4130a12ffd2bb5a9b0f5c932e", "A0900");
			timezoneMap.put("e0185c59c5afa89f317a23426d6b908b", "A0900");
			timezoneMap.put("1eccf317c6a69730444be71e6e190e77", "A0900");
			timezoneMap.put("9b4bda8011584df2a53ee58367317677", "A0900");
			timezoneMap.put("9e65271ce644b8a4492a70e2ff8ea420", "A0900");
			timezoneMap.put("2a1c42001d97a000c1ac729bcacf1a53", "A0900");
			timezoneMap.put("5649b99dd37d05c0c9b9947f1785dac9", "A0900");
			timezoneMap.put("13fc4201c1f1dfc3707052b70c32ca5f", "A0900");
			timezoneMap.put("23f09426a6a523bffcadf9f513682e11", "A0900");
			timezoneMap.put("152392d0682eae51e168e093cce3a6d5", "A0900");
			timezoneMap.put("d37ee6feedf80d656543f809b6ea7aca", "A0900");
			timezoneMap.put("bfc1bd82b0780be8ef490f630b6fc71d", "A0100");
			timezoneMap.put("34fd2ef3e07ceeda5853da0b5c376190", "A0900");
			timezoneMap.put("9726ed6b135ac1f932ad5de26d89fcf8", "A0900");
			timezoneMap.put("0a4a9a4f6db4bcfd14fa3e2bbefc7e57", "A0900");
			timezoneMap.put("dc69dbe328b08cd0fb78035c9758ac65", "A0800");
			timezoneMap.put("beaef04365415872f09107aeb5d964a8", "A0700");
			timezoneMap.put("22fe8d614d94cab7a7f57ce3dd5b9e73", "A0900");
			timezoneMap.put("dbfa9e9adbc2b367114ea94cea45ea1a", "A0900");
			timezoneMap.put("cb5b7a92e8f904c662f60f3fc6f3222f", "A0900");
			timezoneMap.put("053c8dfe40a3f940c1cda34f20981764", "A0900");
			timezoneMap.put("05de25e7b7a4f53af8e512642fc4316b", "A0900");
			timezoneMap.put("eb7ab127b203175ff6f963982cebc428", "A0900");
			timezoneMap.put("6f77a3604413e8b03694333e55a4a850", "A0900");
			timezoneMap.put("36e53d861cd189bea905ea54c141e2a8", "A0900");
			timezoneMap.put("595459ad65f61563433bcbda8b18305b", "A0900");
			timezoneMap.put("6bf5088d7e8b60cb299c6893c8057b4f", "A0800");
			timezoneMap.put("74537f9087127a9cf2139aa92ba2cb15", "A0900");
			timezoneMap.put("3f0f9fe710406c58a300531f9e46a907", "A0900");
			timezoneMap.put("bd3ddd1c2d2eaa9daff807be436e8daf", "A0900");
			timezoneMap.put("3cc08070d7c49e371de9a19d7c44d330", "A0800");
			timezoneMap.put("e7330d5a87ff7aef018a589fc326b849", "A0900");
			timezoneMap.put("6cb9293cef271567dc21b277dce478c7", "A0900");
			timezoneMap.put("ee392ed4999b759bfbc4d0406a8f7d3e", "A0900");
			timezoneMap.put("b1b3be65c897944dd8874d81208f0af7", "A0900");
			timezoneMap.put("b7984a0bb949c4414248c82bbf46e6ed", "A0900");
			timezoneMap.put("9ae762dabe63094dc703b40ef02db2d8", "A0800");
			timezoneMap.put("d7729f61d6d2176b43c74b47b404ea2f", "A0800");
			timezoneMap.put("c41848d30232ed3f9dd8453ce0fabbb1", "A0900");
			timezoneMap.put("91c03a855dbfe7a4d0841f52e83dc714", "A0900");
			timezoneMap.put("39c357fe1aebaaa9fb954aa263fe4403", "A0900");
			timezoneMap.put("ed64e763fd2c5abb135abaeafce066d1", "A0900");
			timezoneMap.put("b2bb11d2ea366aa7a15e3a174b19af78", "A0900");
			timezoneMap.put("432edca313539df0dc2458250811b12a", "A0800");
			timezoneMap.put("7c5a2ac4ea512a361dcfcf7865860a6d", "A0900");
			timezoneMap.put("ddf7eecd1d4fa3fb881f166cc38ddcda", "A0900");
			timezoneMap.put("34081ce7a147473fb3cdfca8ac343060", "B0600");
			timezoneMap.put("d1e40e1c0f4990f6c7a95c43ebb86f8b", "B1000");
			timezoneMap.put("84d3d6b1003f469609a2aa38624f77d0", "A0900");
			timezoneMap.put("33caaebc75cb8098d9bc2fb97eca2442", "A0800");
			timezoneMap.put("a8b91208fd41f8ff00d11d17b69c5fdd", "B0700");
			timezoneMap.put("8ad3469fcca5093f33043372ce961d75", "B0700");
			timezoneMap.put("1044729b122f381b85bcf6825ca56bd8", "A0900");
			timezoneMap.put("fb41945cce7e49a6038c6fc13841e284", "A0800");
			timezoneMap.put("8dd80f92fd637fc825a87e68b12d4c59", "A0900");
			timezoneMap.put("48ce73b4385514154b9cc1a8efa28a0f", "A0900");
			timezoneMap.put("d1c502439a4e8d4fb471e1ba1c0c6814", "A0900");
			timezoneMap.put("eeecc4dec4637ebd5f0a4d4355ccc336", "A0700");
			timezoneMap.put("e3ea586ea17f90636a9e6a450a0b5ca1", "A0900");
			timezoneMap.put("b237683e27ee0e42184c6221b4c37137", "A0800");
			timezoneMap.put("5898ceb9ecdeee6aa0c14adde33e05ff", "A0900");
			timezoneMap.put("7e39663a762d1446f0847b0e189011f2", "A0700");
			timezoneMap.put("96a512b235415c54197abec4c7b4fcbb", "A0900");
			timezoneMap.put("7d5541fd41ac70c302101ae287df0f5e", "A0900");
			timezoneMap.put("fcffca4e28bab5528aeb8d1dfa049303", "A0900");
			timezoneMap.put("d974d9b26df83279d089f750fd3b7828", "A0900");
			timezoneMap.put("033c1e2690d967cd4639d52e9faaa6b3", "A0900");
			timezoneMap.put("3692f841f8eb2fd0f8b083e12d0c8d86", "A0900");
			timezoneMap.put("b0eab779ff407df1184750976622362a", "A0900");
			timezoneMap.put("fbee4c023d78fe0f4d1b9856e0f8056c", "A0900");
			timezoneMap.put("de15731e403425791d76f3a87a38e474", "A0900");
			timezoneMap.put("967ec5df2588c6b65736f9ec2ba2ba81", "A0700");
			timezoneMap.put("79207d96ae5088f633f37976026cb1ec", "A0800");
			timezoneMap.put("7667aa22bcb12b2c0af88550613ffce1", "A0900");
			timezoneMap.put("c1642518c8f630585d7e918b707d0604", "A0900");
			timezoneMap.put("f84cb8a426a2c1cce77a5c9cda2bef77", "A0700");
			timezoneMap.put("cdb4d160bf6fe3c33274981012268e30", "A0900");
			timezoneMap.put("107fe1802b81934499ac200e24172f74", "A0900");
			timezoneMap.put("2bdcf0e292ff75b19ca9e894b181a93b", "A0700");
			timezoneMap.put("5385d325f3260b4f7e5889cfadf9747e", "A0900");
			timezoneMap.put("5f15e70e0e3eaed09f3e07d0eed01073", "A0900");
			timezoneMap.put("72bc9bb60a119469f2f15d3042cd7ee9", "A0800");
			timezoneMap.put("bbbea7511aab5fb361e1724fe5fc5de4", "A0900");
			timezoneMap.put("745b7f16fb058dac00745745ba900190", "A0900");
			timezoneMap.put("1305cbcf83116ffc10cf5e042ee3d40d", "A0800");
			timezoneMap.put("184918da5c3af5ba836bd36391a9eb2a", "A0900");
			timezoneMap.put("1cea03fe65002596a9cbb57834058b0c", "A0900");
			timezoneMap.put("3e3f29888e8f75e6ef7813cdb4e66115", "A0900");
			timezoneMap.put("04e5e2f102e28f5b762db885f2f560b5", "A0900");
			timezoneMap.put("762fd327c2cdbe0005597d859bcd6720", "A0900");
			timezoneMap.put("aa5166609e7020d4ce8293a52bd70f42", "A0900");
			timezoneMap.put("d862367d6c1bd402b103ac49421ee357", "A0900");
			timezoneMap.put("2cd8ffbd7866efcad8e24e4a995354f0", "A0900");
			timezoneMap.put("8113c9928b131da2deca067936f2c107", "A0900");
			timezoneMap.put("c6d109fd430f376fddf9552599aae660", "A0900");
			timezoneMap.put("fdefdd6cc717718295b4bf2d18447800", "A0900");
			timezoneMap.put("6af24076272839ea2587fef10c259021", "A0900");
			timezoneMap.put("1f74157e98b3d2c71bf2b97367112369", "A0900");
			timezoneMap.put("3dfad33edd3db7d45050bf7bc25c9c3f", "A0900");
			timezoneMap.put("bf5aa3f15ee6f966cda093b81caf87d8", "A0900");
			timezoneMap.put("9bdd3756d57a00e8705f5ce1188921ec", "A0900");
			timezoneMap.put("b8cf15ed37d2562f940842e0f8940211", "A0900");
			timezoneMap.put("edb0dcb416cbaf8735b275d4c7b04c59", "A0700");
			timezoneMap.put("9c1b0379e79b4dc71fcf6537963641fd", "A0900");
			timezoneMap.put("c0ad01d1b5fcf2e9bc3e0ce038ee24c4", "A0900");
			timezoneMap.put("1fe3285e959c24fc1be11263cf7aa8d5", "A0900");
			timezoneMap.put("d3a37c10a95ed9b94ec462ee62853e77", "A0900");
			timezoneMap.put("f953f97d91ebdcaf122fe041bd44f921", "A0900");
			timezoneMap.put("2c4cae53fd36d83f34da66e25eb473f3", "A0900");
			timezoneMap.put("67d86501427397952f8c230a45b8e64a", "A0800");
			timezoneMap.put("36d1f58a26a2cf3400f5382cd76bc518", "B0600");
			timezoneMap.put("dc1eff1f5fc5642bc0f4de8bcbb31bf7", "A0700");
			timezoneMap.put("544b166f33a4dfa306216d0908f15f1b", "A0900");
			timezoneMap.put("42fcee05169cb1336f1edaa33058b771", "A0900");
			timezoneMap.put("984b6f43ff5f77fbc4ee91c352cd9026", "A0700");
			timezoneMap.put("92a04a0002dd4eafb9235c73b7ebd1e5", "A0700");
			timezoneMap.put("87f955e0f30fe58d3cfcba39194eca00", "A0700");
			timezoneMap.put("eea414838fabba0c491697425a859e69", "A0700");
			timezoneMap.put("97cf6b2c8e74639ae7bfc464ae03f7db", "A0900");
			timezoneMap.put("74b22522e15633777033e19a072d6fb5", "A0700");
			timezoneMap.put("d52079e6579bf2c186a7e80e1171aab2", "A0800");
			timezoneMap.put("c857669bea0f2c52f02f807b0e8404b7", "A0700");
			timezoneMap.put("2d0c73dd32d4edfd574e6e545a49905d", "A0700");
			timezoneMap.put("9666f9668a4db516c8aaea439464da44", "B0800");
			timezoneMap.put("9dbec365f981d5f767c17d06597af047", "A0700");
			timezoneMap.put("61540711742f88fcbb981169b4d8831a", "A0900");
			timezoneMap.put("dca04f9ec3d689c3b8125019cd7966e6", "A0700");
			timezoneMap.put("4a9492e6f79f717034f6d7946e294218", "A0900");
			timezoneMap.put("af8026354a47e51db4cda35a3de46ee3", "A0900");
			timezoneMap.put("e29c58cb30d2a8cfc31579ed7087c108", "A0900");
			timezoneMap.put("2474f9ac2203fb57690e8f21789ea02d", "A0700");
			timezoneMap.put("45ee78121db4e96edf1d6cae41e7f507", "A0700");
			timezoneMap.put("e12107b7720b9efe6f38f6e52cf2ff8f", "A0900");
			timezoneMap.put("a45b30f0ee24cd23ef8ce3081a548bcf", "A0700");
			timezoneMap.put("be07d426e30057fc87c7648d8159509c", "A0800");
			timezoneMap.put("51312342ad1e4d129d82c4dfa27a22f3", "A0700");
			timezoneMap.put("4f4f1c61ef87d5f7967ab0993129dcd6", "A0700");
			timezoneMap.put("b474d02fd5dc9824ec14dfd612105c53", "A0900");
			timezoneMap.put("5befff3e6fdf6e4d42a5ced157427fec", "A0900");
			timezoneMap.put("019fc674980484e26eec1b3a7d1b7a55", "A0700");
			timezoneMap.put("4353d7f8c82ca65c2aeebcd61d489829", "A0800");
			timezoneMap.put("1882be64cf0ec6f868ff0eddbff4b8a6", "A0900");
			timezoneMap.put("57b29b98260ff4c07f5e126ff5b14ad2", "B0800");
			timezoneMap.put("ed6cbc7ac2d46fd19ed67bd9fad5f27d", "A0900");
			timezoneMap.put("6c67d9101a816b567ea63e96ae354076", "A0700");
			timezoneMap.put("00416faf3cc2af8762fd6486a727b390", "A0700");
			timezoneMap.put("01cbe16a808696d4ac4b5da6c42f5d13", "A0700");
			timezoneMap.put("99d3a62c0c903561300d3c64693309bd", "A0700");
			timezoneMap.put("3f81a3c6a1ec90289488a74d271d500e", "A0700");
			timezoneMap.put("4b4bca0a724b6ccc333ec78f7577f2d0", "A0900");
			timezoneMap.put("829d4d86a88a710e3cd92783aa454866", "A0700");
			timezoneMap.put("ec6393b33909b39a97d6dc1479beab55", "A0900");
			timezoneMap.put("59ae6e4aa7b120f7ddc8a94300dcdd06", "A0700");
			timezoneMap.put("e71ed06383cfc3f5efd6957e05c5a10e", "A0700");
			timezoneMap.put("9b52c157ee245a9c42d9c389e6846b5a", "A0900");
			timezoneMap.put("919b447f5e95ea6a172cc34628bf0787", "A0900");
			timezoneMap.put("8ad55e262d97f6a9b633e62ad41c2b1d", "A0700");
			timezoneMap.put("8415ce26e3a15604472c6e85305b1184", "A0700");
			timezoneMap.put("f7e83b072c9c2b31af489e84f4e5c484", "A0900");
			timezoneMap.put("bb25f60c434c0150f0703e26f4554b97", "A0700");
			timezoneMap.put("a9b0bba517f0e1a34338990247e43f3d", "A0900");
			timezoneMap.put("94076ae8e346d384445d7f1048a490ac", "A0900");
			timezoneMap.put("95ab28f5665484d8ceef2ad25421fdd4", "A0900");
			timezoneMap.put("0bc6bcd152e209dbf6db8d52f8856549", "A0900");
			timezoneMap.put("292743e09875a348de514fbc76aff179", "A0900");
			timezoneMap.put("4d40326894005a5184b92c6df6dfef78", "A0700");
			timezoneMap.put("209fd8602db521e9c665e9af8e051acd", "A0700");
			timezoneMap.put("88575f7c70825a505df6e34dbf98afd0", "A0700");
			timezoneMap.put("1d41344761613ff88d9f7407c18d47cf", "A0900");
			timezoneMap.put("61a6b727f20ac1aaea43f7cba3685114", "A0800");
			timezoneMap.put("fbc2a1efe0d941d4310ee1866db3dba5", "A0900");
			timezoneMap.put("35e3d7c0afa2b4694956907e039befef", "A0900");
			timezoneMap.put("0f4d910d5fff96890a4abd47a43fc77d", "B0600");
			timezoneMap.put("b3e5329d3668782175c86869686ad137", "A0900");
			timezoneMap.put("322cc5b921da9c41780b7300bf744c70", "A0900");
			timezoneMap.put("a6b7a863b6a318de7c247d19cd45789f", "A0900");
			timezoneMap.put("037ab7fad9b33cf8aaf991ad12d95491", "A0900");
			timezoneMap.put("0155c202a95c369c7724b7aa4e991dcc", "A0900");
			timezoneMap.put("5ffb3d1b63b69f290839c610140c772c", "A0900");
			timezoneMap.put("d6a5f313a6a9a6d48159522bc11023f8", "1100");
			timezoneMap.put("439746b8706ef7f30cb49b440fd34fa5", "A0900");
			timezoneMap.put("3fdb805b9b78964b10142543788fe495", "A0900");
			timezoneMap.put("7379ac6d610f7aa2d296aa750a14bab8", "A0900");
			timezoneMap.put("02b7ca4687a092e141153898d4015b19", "A0900");
			timezoneMap.put("8b50e22ff6133589ef8c6952e176ccc4", "A0900");
			timezoneMap.put("ccd626f07b3be31ea794187283635500", "A0900");
			timezoneMap.put("05eea5b19798e5a72a28f584f100aacd", "A0700");
			timezoneMap.put("2b518dea95a1f555f19c4aa691a4a461", "A0900");
			timezoneMap.put("1ebac38777816136a17fe94bdd25608c", "A0900");
			timezoneMap.put("d800ceeec176b9fe9a832a361f980c56", "A0900");
			timezoneMap.put("6de38a759e66a56e4916d0eca84b3e5b", "A0900");
			timezoneMap.put("4ceffb68702323668d057ddad179e602", "A0700");
			timezoneMap.put("6b5a3d4730fa82668062570f9dd8f744", "A0700");
			timezoneMap.put("ab4a576e4aada4a0daaff8a84ba98faf", "A0900");
			timezoneMap.put("3f69079a298fa9083109393246608e90", "A0900");
			timezoneMap.put("6ad36b421be2d5a06a0b04f7dd8f9fb8", "A0900");
			timezoneMap.put("78a8c5437f8dbabb73c484d2eb5c8ae3", "A0900");
			timezoneMap.put("8d2358cae8e8e561d4465133192f13eb", "A0900");
			timezoneMap.put("9319204f095692aec8bd2476e79891df", "A0900");
			timezoneMap.put("b8804b8f49d606d2679a490a4f3f8062", "A0900");
			timezoneMap.put("ba80c6dbe9f121f3e88ee58ba1ad9340", "A0800");
			timezoneMap.put("9787952a364626ea1a9de946e1d1dc64", "A0900");
			timezoneMap.put("8c9628e80ad58a9183e5839e7214db44", "A0900");
			timezoneMap.put("5e20fa4b064869d999cdc4932d44927b", "A0900");
			timezoneMap.put("5120c754ea7563a1b6c3914453f10668", "A0900");
			timezoneMap.put("08727c3dbb7d37eb1d3e0bc0db15b047", "A0900");
			timezoneMap.put("68f192d1d725d29376c0068954b295ed", "A0900");
			timezoneMap.put("56d4505afda591d86823a5481598d1fc", "A0100");
			timezoneMap.put("4807d0a0873c3e4895e8aaa705ce2158", "A0100");
			timezoneMap.put("530ddc98ef396721ea4c193ca7766458", "A0900");
			timezoneMap.put("cd98f40c57f283a7dc2a7b9e886a07b0", "A0900");
			timezoneMap.put("9773a125b116972814998ebd2e8450b9", "A0700");
			timezoneMap.put("ce04478d552b2af2609d4f0645cde20c", "A0900");
			timezoneMap.put("46b1b5e46e1feebfeb0a136e386ffc0b", "A0700");
			timezoneMap.put("3b8193e132c8af2312c54fd73fe87a83", "A0900");
			timezoneMap.put("a23e79a5d91cede580f48f82ccfc35e2", "A0900");
			timezoneMap.put("785743b2c10006504bc4755b338d78bc", "A0700");
			timezoneMap.put("29c64b1c83c076d6581ed3df157b6084", "A0900");
			timezoneMap.put("d72e28ff9ce918a10cf673b79e726518", "A0900");
			timezoneMap.put("e760276d463e1f44c339975373935782", "A0900");
			timezoneMap.put("89dd85264639c08b14f4946e59232fbe", "A0800");
			timezoneMap.put("43999f9617170d17bd2d7eff15b6d30d", "A0700");
			timezoneMap.put("461154f3bf5df28a4f611d1df5f197af", "A0900");
			timezoneMap.put("8c7360839df4a45efd69f48571178f93", "A0900");
			timezoneMap.put("e5ac75c7d2c1d03f0036a857416fd310", "A0900");
			timezoneMap.put("7d02d96a45fc023c3b41858781e9a4b2", "A0900");
			timezoneMap.put("77fa50ffe808bfb7bd6c64fe3c389769", "A0900");
			timezoneMap.put("6671511ae690ef1409a87890916138ed", "A0700");
			timezoneMap.put("8fb0270f035f5c270e9f067275985ce9", "A0900");
			timezoneMap.put("bb8139f6266761c5fc8f4c21a729ad17", "B0800");
			timezoneMap.put("61f7e6ded73bd8574013c82ea4274a5a", "A0900");
			timezoneMap.put("8a6c9106c8c3f685e0c220c5bddf339d", "A0900");
			timezoneMap.put("a190b94b6fddb32f8d22da578e15a415", "A0900");
			timezoneMap.put("d51eda4008943ef47b88d5d657e2af4a", "A0900");
			timezoneMap.put("0b0ee32440ed0dfbebd69eed23bb69cc", "A0900");
			timezoneMap.put("32da84a143a41fc9cee0a14ad8e0e261", "A0900");
			timezoneMap.put("8ba77a00a2a1350567925d579370ade7", "A0900");
			timezoneMap.put("6f26a952a3ff51fb5a5b685667699ead", "A0700");
			timezoneMap.put("6e537009e567da5f020afbce0f90e027", "A0900");
			timezoneMap.put("df138a6c6e986470880f2952683fb7cf", "A0700");
			timezoneMap.put("29e5df137c26c17c7f30b7cc2405679d", "A0900");
			timezoneMap.put("80ed07a16f0806d19d545781380f6039", "A0900");
			timezoneMap.put("5edae859089c2e9234482aa11bba7563", "A0900");
			timezoneMap.put("917ca795e872d9794c93cc71253b55ce", "A0900");
			timezoneMap.put("85dd572e85e6299fbc899d9ac98f38c3", "A0900");
		}

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] keys = LineDataTool.asFields(key.toString());
			String timezone = timezoneMap.get(keys[0]);

			if (timezone == null) {
				timezone = "unknown";
			}

			k.set(LineDataTool.asLine(new String[] { keys[0], keys[1], timezone }));
			
			context.write(k, value);
		}
	}

	static class TimezoneReducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		private Text k = new Text();

		@Override
		public void setup(Context context) {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keys = LineDataTool.asFields(key.toString());
			String timezone = keys[2];

			k.set(LineDataTool.asLine(new String[] { keys[0], keys[1] }));
			for (Text value : values) {
				mos.write(k, value, String.format("%s/part", timezone));
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
}
