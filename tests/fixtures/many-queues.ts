import { z } from 'zod'
import { defineQueue } from '../../src/queue.js'

/** 400 literal queues: `defineQueues` must stay linear in their count. */
export const manyQueues = [
	defineQueue({
		name: 'q0',
		inputSchema: z.object({ id: z.string(), n0: z.number() })
	}),
	defineQueue({
		name: 'q1',
		inputSchema: z.object({ id: z.string(), n1: z.number() })
	}),
	defineQueue({
		name: 'q2',
		inputSchema: z.object({ id: z.string(), n2: z.number() })
	}),
	defineQueue({
		name: 'q3',
		inputSchema: z.object({ id: z.string(), n3: z.number() })
	}),
	defineQueue({
		name: 'q4',
		inputSchema: z.object({ id: z.string(), n4: z.number() })
	}),
	defineQueue({
		name: 'q5',
		inputSchema: z.object({ id: z.string(), n5: z.number() })
	}),
	defineQueue({
		name: 'q6',
		inputSchema: z.object({ id: z.string(), n6: z.number() })
	}),
	defineQueue({
		name: 'q7',
		inputSchema: z.object({ id: z.string(), n7: z.number() })
	}),
	defineQueue({
		name: 'q8',
		inputSchema: z.object({ id: z.string(), n8: z.number() })
	}),
	defineQueue({
		name: 'q9',
		inputSchema: z.object({ id: z.string(), n9: z.number() })
	}),
	defineQueue({
		name: 'q10',
		inputSchema: z.object({ id: z.string(), n10: z.number() })
	}),
	defineQueue({
		name: 'q11',
		inputSchema: z.object({ id: z.string(), n11: z.number() })
	}),
	defineQueue({
		name: 'q12',
		inputSchema: z.object({ id: z.string(), n12: z.number() })
	}),
	defineQueue({
		name: 'q13',
		inputSchema: z.object({ id: z.string(), n13: z.number() })
	}),
	defineQueue({
		name: 'q14',
		inputSchema: z.object({ id: z.string(), n14: z.number() })
	}),
	defineQueue({
		name: 'q15',
		inputSchema: z.object({ id: z.string(), n15: z.number() })
	}),
	defineQueue({
		name: 'q16',
		inputSchema: z.object({ id: z.string(), n16: z.number() })
	}),
	defineQueue({
		name: 'q17',
		inputSchema: z.object({ id: z.string(), n17: z.number() })
	}),
	defineQueue({
		name: 'q18',
		inputSchema: z.object({ id: z.string(), n18: z.number() })
	}),
	defineQueue({
		name: 'q19',
		inputSchema: z.object({ id: z.string(), n19: z.number() })
	}),
	defineQueue({
		name: 'q20',
		inputSchema: z.object({ id: z.string(), n20: z.number() })
	}),
	defineQueue({
		name: 'q21',
		inputSchema: z.object({ id: z.string(), n21: z.number() })
	}),
	defineQueue({
		name: 'q22',
		inputSchema: z.object({ id: z.string(), n22: z.number() })
	}),
	defineQueue({
		name: 'q23',
		inputSchema: z.object({ id: z.string(), n23: z.number() })
	}),
	defineQueue({
		name: 'q24',
		inputSchema: z.object({ id: z.string(), n24: z.number() })
	}),
	defineQueue({
		name: 'q25',
		inputSchema: z.object({ id: z.string(), n25: z.number() })
	}),
	defineQueue({
		name: 'q26',
		inputSchema: z.object({ id: z.string(), n26: z.number() })
	}),
	defineQueue({
		name: 'q27',
		inputSchema: z.object({ id: z.string(), n27: z.number() })
	}),
	defineQueue({
		name: 'q28',
		inputSchema: z.object({ id: z.string(), n28: z.number() })
	}),
	defineQueue({
		name: 'q29',
		inputSchema: z.object({ id: z.string(), n29: z.number() })
	}),
	defineQueue({
		name: 'q30',
		inputSchema: z.object({ id: z.string(), n30: z.number() })
	}),
	defineQueue({
		name: 'q31',
		inputSchema: z.object({ id: z.string(), n31: z.number() })
	}),
	defineQueue({
		name: 'q32',
		inputSchema: z.object({ id: z.string(), n32: z.number() })
	}),
	defineQueue({
		name: 'q33',
		inputSchema: z.object({ id: z.string(), n33: z.number() })
	}),
	defineQueue({
		name: 'q34',
		inputSchema: z.object({ id: z.string(), n34: z.number() })
	}),
	defineQueue({
		name: 'q35',
		inputSchema: z.object({ id: z.string(), n35: z.number() })
	}),
	defineQueue({
		name: 'q36',
		inputSchema: z.object({ id: z.string(), n36: z.number() })
	}),
	defineQueue({
		name: 'q37',
		inputSchema: z.object({ id: z.string(), n37: z.number() })
	}),
	defineQueue({
		name: 'q38',
		inputSchema: z.object({ id: z.string(), n38: z.number() })
	}),
	defineQueue({
		name: 'q39',
		inputSchema: z.object({ id: z.string(), n39: z.number() })
	}),
	defineQueue({
		name: 'q40',
		inputSchema: z.object({ id: z.string(), n40: z.number() })
	}),
	defineQueue({
		name: 'q41',
		inputSchema: z.object({ id: z.string(), n41: z.number() })
	}),
	defineQueue({
		name: 'q42',
		inputSchema: z.object({ id: z.string(), n42: z.number() })
	}),
	defineQueue({
		name: 'q43',
		inputSchema: z.object({ id: z.string(), n43: z.number() })
	}),
	defineQueue({
		name: 'q44',
		inputSchema: z.object({ id: z.string(), n44: z.number() })
	}),
	defineQueue({
		name: 'q45',
		inputSchema: z.object({ id: z.string(), n45: z.number() })
	}),
	defineQueue({
		name: 'q46',
		inputSchema: z.object({ id: z.string(), n46: z.number() })
	}),
	defineQueue({
		name: 'q47',
		inputSchema: z.object({ id: z.string(), n47: z.number() })
	}),
	defineQueue({
		name: 'q48',
		inputSchema: z.object({ id: z.string(), n48: z.number() })
	}),
	defineQueue({
		name: 'q49',
		inputSchema: z.object({ id: z.string(), n49: z.number() })
	}),
	defineQueue({
		name: 'q50',
		inputSchema: z.object({ id: z.string(), n50: z.number() })
	}),
	defineQueue({
		name: 'q51',
		inputSchema: z.object({ id: z.string(), n51: z.number() })
	}),
	defineQueue({
		name: 'q52',
		inputSchema: z.object({ id: z.string(), n52: z.number() })
	}),
	defineQueue({
		name: 'q53',
		inputSchema: z.object({ id: z.string(), n53: z.number() })
	}),
	defineQueue({
		name: 'q54',
		inputSchema: z.object({ id: z.string(), n54: z.number() })
	}),
	defineQueue({
		name: 'q55',
		inputSchema: z.object({ id: z.string(), n55: z.number() })
	}),
	defineQueue({
		name: 'q56',
		inputSchema: z.object({ id: z.string(), n56: z.number() })
	}),
	defineQueue({
		name: 'q57',
		inputSchema: z.object({ id: z.string(), n57: z.number() })
	}),
	defineQueue({
		name: 'q58',
		inputSchema: z.object({ id: z.string(), n58: z.number() })
	}),
	defineQueue({
		name: 'q59',
		inputSchema: z.object({ id: z.string(), n59: z.number() })
	}),
	defineQueue({
		name: 'q60',
		inputSchema: z.object({ id: z.string(), n60: z.number() })
	}),
	defineQueue({
		name: 'q61',
		inputSchema: z.object({ id: z.string(), n61: z.number() })
	}),
	defineQueue({
		name: 'q62',
		inputSchema: z.object({ id: z.string(), n62: z.number() })
	}),
	defineQueue({
		name: 'q63',
		inputSchema: z.object({ id: z.string(), n63: z.number() })
	}),
	defineQueue({
		name: 'q64',
		inputSchema: z.object({ id: z.string(), n64: z.number() })
	}),
	defineQueue({
		name: 'q65',
		inputSchema: z.object({ id: z.string(), n65: z.number() })
	}),
	defineQueue({
		name: 'q66',
		inputSchema: z.object({ id: z.string(), n66: z.number() })
	}),
	defineQueue({
		name: 'q67',
		inputSchema: z.object({ id: z.string(), n67: z.number() })
	}),
	defineQueue({
		name: 'q68',
		inputSchema: z.object({ id: z.string(), n68: z.number() })
	}),
	defineQueue({
		name: 'q69',
		inputSchema: z.object({ id: z.string(), n69: z.number() })
	}),
	defineQueue({
		name: 'q70',
		inputSchema: z.object({ id: z.string(), n70: z.number() })
	}),
	defineQueue({
		name: 'q71',
		inputSchema: z.object({ id: z.string(), n71: z.number() })
	}),
	defineQueue({
		name: 'q72',
		inputSchema: z.object({ id: z.string(), n72: z.number() })
	}),
	defineQueue({
		name: 'q73',
		inputSchema: z.object({ id: z.string(), n73: z.number() })
	}),
	defineQueue({
		name: 'q74',
		inputSchema: z.object({ id: z.string(), n74: z.number() })
	}),
	defineQueue({
		name: 'q75',
		inputSchema: z.object({ id: z.string(), n75: z.number() })
	}),
	defineQueue({
		name: 'q76',
		inputSchema: z.object({ id: z.string(), n76: z.number() })
	}),
	defineQueue({
		name: 'q77',
		inputSchema: z.object({ id: z.string(), n77: z.number() })
	}),
	defineQueue({
		name: 'q78',
		inputSchema: z.object({ id: z.string(), n78: z.number() })
	}),
	defineQueue({
		name: 'q79',
		inputSchema: z.object({ id: z.string(), n79: z.number() })
	}),
	defineQueue({
		name: 'q80',
		inputSchema: z.object({ id: z.string(), n80: z.number() })
	}),
	defineQueue({
		name: 'q81',
		inputSchema: z.object({ id: z.string(), n81: z.number() })
	}),
	defineQueue({
		name: 'q82',
		inputSchema: z.object({ id: z.string(), n82: z.number() })
	}),
	defineQueue({
		name: 'q83',
		inputSchema: z.object({ id: z.string(), n83: z.number() })
	}),
	defineQueue({
		name: 'q84',
		inputSchema: z.object({ id: z.string(), n84: z.number() })
	}),
	defineQueue({
		name: 'q85',
		inputSchema: z.object({ id: z.string(), n85: z.number() })
	}),
	defineQueue({
		name: 'q86',
		inputSchema: z.object({ id: z.string(), n86: z.number() })
	}),
	defineQueue({
		name: 'q87',
		inputSchema: z.object({ id: z.string(), n87: z.number() })
	}),
	defineQueue({
		name: 'q88',
		inputSchema: z.object({ id: z.string(), n88: z.number() })
	}),
	defineQueue({
		name: 'q89',
		inputSchema: z.object({ id: z.string(), n89: z.number() })
	}),
	defineQueue({
		name: 'q90',
		inputSchema: z.object({ id: z.string(), n90: z.number() })
	}),
	defineQueue({
		name: 'q91',
		inputSchema: z.object({ id: z.string(), n91: z.number() })
	}),
	defineQueue({
		name: 'q92',
		inputSchema: z.object({ id: z.string(), n92: z.number() })
	}),
	defineQueue({
		name: 'q93',
		inputSchema: z.object({ id: z.string(), n93: z.number() })
	}),
	defineQueue({
		name: 'q94',
		inputSchema: z.object({ id: z.string(), n94: z.number() })
	}),
	defineQueue({
		name: 'q95',
		inputSchema: z.object({ id: z.string(), n95: z.number() })
	}),
	defineQueue({
		name: 'q96',
		inputSchema: z.object({ id: z.string(), n96: z.number() })
	}),
	defineQueue({
		name: 'q97',
		inputSchema: z.object({ id: z.string(), n97: z.number() })
	}),
	defineQueue({
		name: 'q98',
		inputSchema: z.object({ id: z.string(), n98: z.number() })
	}),
	defineQueue({
		name: 'q99',
		inputSchema: z.object({ id: z.string(), n99: z.number() })
	}),
	defineQueue({
		name: 'q100',
		inputSchema: z.object({ id: z.string(), n100: z.number() })
	}),
	defineQueue({
		name: 'q101',
		inputSchema: z.object({ id: z.string(), n101: z.number() })
	}),
	defineQueue({
		name: 'q102',
		inputSchema: z.object({ id: z.string(), n102: z.number() })
	}),
	defineQueue({
		name: 'q103',
		inputSchema: z.object({ id: z.string(), n103: z.number() })
	}),
	defineQueue({
		name: 'q104',
		inputSchema: z.object({ id: z.string(), n104: z.number() })
	}),
	defineQueue({
		name: 'q105',
		inputSchema: z.object({ id: z.string(), n105: z.number() })
	}),
	defineQueue({
		name: 'q106',
		inputSchema: z.object({ id: z.string(), n106: z.number() })
	}),
	defineQueue({
		name: 'q107',
		inputSchema: z.object({ id: z.string(), n107: z.number() })
	}),
	defineQueue({
		name: 'q108',
		inputSchema: z.object({ id: z.string(), n108: z.number() })
	}),
	defineQueue({
		name: 'q109',
		inputSchema: z.object({ id: z.string(), n109: z.number() })
	}),
	defineQueue({
		name: 'q110',
		inputSchema: z.object({ id: z.string(), n110: z.number() })
	}),
	defineQueue({
		name: 'q111',
		inputSchema: z.object({ id: z.string(), n111: z.number() })
	}),
	defineQueue({
		name: 'q112',
		inputSchema: z.object({ id: z.string(), n112: z.number() })
	}),
	defineQueue({
		name: 'q113',
		inputSchema: z.object({ id: z.string(), n113: z.number() })
	}),
	defineQueue({
		name: 'q114',
		inputSchema: z.object({ id: z.string(), n114: z.number() })
	}),
	defineQueue({
		name: 'q115',
		inputSchema: z.object({ id: z.string(), n115: z.number() })
	}),
	defineQueue({
		name: 'q116',
		inputSchema: z.object({ id: z.string(), n116: z.number() })
	}),
	defineQueue({
		name: 'q117',
		inputSchema: z.object({ id: z.string(), n117: z.number() })
	}),
	defineQueue({
		name: 'q118',
		inputSchema: z.object({ id: z.string(), n118: z.number() })
	}),
	defineQueue({
		name: 'q119',
		inputSchema: z.object({ id: z.string(), n119: z.number() })
	}),
	defineQueue({
		name: 'q120',
		inputSchema: z.object({ id: z.string(), n120: z.number() })
	}),
	defineQueue({
		name: 'q121',
		inputSchema: z.object({ id: z.string(), n121: z.number() })
	}),
	defineQueue({
		name: 'q122',
		inputSchema: z.object({ id: z.string(), n122: z.number() })
	}),
	defineQueue({
		name: 'q123',
		inputSchema: z.object({ id: z.string(), n123: z.number() })
	}),
	defineQueue({
		name: 'q124',
		inputSchema: z.object({ id: z.string(), n124: z.number() })
	}),
	defineQueue({
		name: 'q125',
		inputSchema: z.object({ id: z.string(), n125: z.number() })
	}),
	defineQueue({
		name: 'q126',
		inputSchema: z.object({ id: z.string(), n126: z.number() })
	}),
	defineQueue({
		name: 'q127',
		inputSchema: z.object({ id: z.string(), n127: z.number() })
	}),
	defineQueue({
		name: 'q128',
		inputSchema: z.object({ id: z.string(), n128: z.number() })
	}),
	defineQueue({
		name: 'q129',
		inputSchema: z.object({ id: z.string(), n129: z.number() })
	}),
	defineQueue({
		name: 'q130',
		inputSchema: z.object({ id: z.string(), n130: z.number() })
	}),
	defineQueue({
		name: 'q131',
		inputSchema: z.object({ id: z.string(), n131: z.number() })
	}),
	defineQueue({
		name: 'q132',
		inputSchema: z.object({ id: z.string(), n132: z.number() })
	}),
	defineQueue({
		name: 'q133',
		inputSchema: z.object({ id: z.string(), n133: z.number() })
	}),
	defineQueue({
		name: 'q134',
		inputSchema: z.object({ id: z.string(), n134: z.number() })
	}),
	defineQueue({
		name: 'q135',
		inputSchema: z.object({ id: z.string(), n135: z.number() })
	}),
	defineQueue({
		name: 'q136',
		inputSchema: z.object({ id: z.string(), n136: z.number() })
	}),
	defineQueue({
		name: 'q137',
		inputSchema: z.object({ id: z.string(), n137: z.number() })
	}),
	defineQueue({
		name: 'q138',
		inputSchema: z.object({ id: z.string(), n138: z.number() })
	}),
	defineQueue({
		name: 'q139',
		inputSchema: z.object({ id: z.string(), n139: z.number() })
	}),
	defineQueue({
		name: 'q140',
		inputSchema: z.object({ id: z.string(), n140: z.number() })
	}),
	defineQueue({
		name: 'q141',
		inputSchema: z.object({ id: z.string(), n141: z.number() })
	}),
	defineQueue({
		name: 'q142',
		inputSchema: z.object({ id: z.string(), n142: z.number() })
	}),
	defineQueue({
		name: 'q143',
		inputSchema: z.object({ id: z.string(), n143: z.number() })
	}),
	defineQueue({
		name: 'q144',
		inputSchema: z.object({ id: z.string(), n144: z.number() })
	}),
	defineQueue({
		name: 'q145',
		inputSchema: z.object({ id: z.string(), n145: z.number() })
	}),
	defineQueue({
		name: 'q146',
		inputSchema: z.object({ id: z.string(), n146: z.number() })
	}),
	defineQueue({
		name: 'q147',
		inputSchema: z.object({ id: z.string(), n147: z.number() })
	}),
	defineQueue({
		name: 'q148',
		inputSchema: z.object({ id: z.string(), n148: z.number() })
	}),
	defineQueue({
		name: 'q149',
		inputSchema: z.object({ id: z.string(), n149: z.number() })
	}),
	defineQueue({
		name: 'q150',
		inputSchema: z.object({ id: z.string(), n150: z.number() })
	}),
	defineQueue({
		name: 'q151',
		inputSchema: z.object({ id: z.string(), n151: z.number() })
	}),
	defineQueue({
		name: 'q152',
		inputSchema: z.object({ id: z.string(), n152: z.number() })
	}),
	defineQueue({
		name: 'q153',
		inputSchema: z.object({ id: z.string(), n153: z.number() })
	}),
	defineQueue({
		name: 'q154',
		inputSchema: z.object({ id: z.string(), n154: z.number() })
	}),
	defineQueue({
		name: 'q155',
		inputSchema: z.object({ id: z.string(), n155: z.number() })
	}),
	defineQueue({
		name: 'q156',
		inputSchema: z.object({ id: z.string(), n156: z.number() })
	}),
	defineQueue({
		name: 'q157',
		inputSchema: z.object({ id: z.string(), n157: z.number() })
	}),
	defineQueue({
		name: 'q158',
		inputSchema: z.object({ id: z.string(), n158: z.number() })
	}),
	defineQueue({
		name: 'q159',
		inputSchema: z.object({ id: z.string(), n159: z.number() })
	}),
	defineQueue({
		name: 'q160',
		inputSchema: z.object({ id: z.string(), n160: z.number() })
	}),
	defineQueue({
		name: 'q161',
		inputSchema: z.object({ id: z.string(), n161: z.number() })
	}),
	defineQueue({
		name: 'q162',
		inputSchema: z.object({ id: z.string(), n162: z.number() })
	}),
	defineQueue({
		name: 'q163',
		inputSchema: z.object({ id: z.string(), n163: z.number() })
	}),
	defineQueue({
		name: 'q164',
		inputSchema: z.object({ id: z.string(), n164: z.number() })
	}),
	defineQueue({
		name: 'q165',
		inputSchema: z.object({ id: z.string(), n165: z.number() })
	}),
	defineQueue({
		name: 'q166',
		inputSchema: z.object({ id: z.string(), n166: z.number() })
	}),
	defineQueue({
		name: 'q167',
		inputSchema: z.object({ id: z.string(), n167: z.number() })
	}),
	defineQueue({
		name: 'q168',
		inputSchema: z.object({ id: z.string(), n168: z.number() })
	}),
	defineQueue({
		name: 'q169',
		inputSchema: z.object({ id: z.string(), n169: z.number() })
	}),
	defineQueue({
		name: 'q170',
		inputSchema: z.object({ id: z.string(), n170: z.number() })
	}),
	defineQueue({
		name: 'q171',
		inputSchema: z.object({ id: z.string(), n171: z.number() })
	}),
	defineQueue({
		name: 'q172',
		inputSchema: z.object({ id: z.string(), n172: z.number() })
	}),
	defineQueue({
		name: 'q173',
		inputSchema: z.object({ id: z.string(), n173: z.number() })
	}),
	defineQueue({
		name: 'q174',
		inputSchema: z.object({ id: z.string(), n174: z.number() })
	}),
	defineQueue({
		name: 'q175',
		inputSchema: z.object({ id: z.string(), n175: z.number() })
	}),
	defineQueue({
		name: 'q176',
		inputSchema: z.object({ id: z.string(), n176: z.number() })
	}),
	defineQueue({
		name: 'q177',
		inputSchema: z.object({ id: z.string(), n177: z.number() })
	}),
	defineQueue({
		name: 'q178',
		inputSchema: z.object({ id: z.string(), n178: z.number() })
	}),
	defineQueue({
		name: 'q179',
		inputSchema: z.object({ id: z.string(), n179: z.number() })
	}),
	defineQueue({
		name: 'q180',
		inputSchema: z.object({ id: z.string(), n180: z.number() })
	}),
	defineQueue({
		name: 'q181',
		inputSchema: z.object({ id: z.string(), n181: z.number() })
	}),
	defineQueue({
		name: 'q182',
		inputSchema: z.object({ id: z.string(), n182: z.number() })
	}),
	defineQueue({
		name: 'q183',
		inputSchema: z.object({ id: z.string(), n183: z.number() })
	}),
	defineQueue({
		name: 'q184',
		inputSchema: z.object({ id: z.string(), n184: z.number() })
	}),
	defineQueue({
		name: 'q185',
		inputSchema: z.object({ id: z.string(), n185: z.number() })
	}),
	defineQueue({
		name: 'q186',
		inputSchema: z.object({ id: z.string(), n186: z.number() })
	}),
	defineQueue({
		name: 'q187',
		inputSchema: z.object({ id: z.string(), n187: z.number() })
	}),
	defineQueue({
		name: 'q188',
		inputSchema: z.object({ id: z.string(), n188: z.number() })
	}),
	defineQueue({
		name: 'q189',
		inputSchema: z.object({ id: z.string(), n189: z.number() })
	}),
	defineQueue({
		name: 'q190',
		inputSchema: z.object({ id: z.string(), n190: z.number() })
	}),
	defineQueue({
		name: 'q191',
		inputSchema: z.object({ id: z.string(), n191: z.number() })
	}),
	defineQueue({
		name: 'q192',
		inputSchema: z.object({ id: z.string(), n192: z.number() })
	}),
	defineQueue({
		name: 'q193',
		inputSchema: z.object({ id: z.string(), n193: z.number() })
	}),
	defineQueue({
		name: 'q194',
		inputSchema: z.object({ id: z.string(), n194: z.number() })
	}),
	defineQueue({
		name: 'q195',
		inputSchema: z.object({ id: z.string(), n195: z.number() })
	}),
	defineQueue({
		name: 'q196',
		inputSchema: z.object({ id: z.string(), n196: z.number() })
	}),
	defineQueue({
		name: 'q197',
		inputSchema: z.object({ id: z.string(), n197: z.number() })
	}),
	defineQueue({
		name: 'q198',
		inputSchema: z.object({ id: z.string(), n198: z.number() })
	}),
	defineQueue({
		name: 'q199',
		inputSchema: z.object({ id: z.string(), n199: z.number() })
	}),
	defineQueue({
		name: 'q200',
		inputSchema: z.object({ id: z.string(), n200: z.number() })
	}),
	defineQueue({
		name: 'q201',
		inputSchema: z.object({ id: z.string(), n201: z.number() })
	}),
	defineQueue({
		name: 'q202',
		inputSchema: z.object({ id: z.string(), n202: z.number() })
	}),
	defineQueue({
		name: 'q203',
		inputSchema: z.object({ id: z.string(), n203: z.number() })
	}),
	defineQueue({
		name: 'q204',
		inputSchema: z.object({ id: z.string(), n204: z.number() })
	}),
	defineQueue({
		name: 'q205',
		inputSchema: z.object({ id: z.string(), n205: z.number() })
	}),
	defineQueue({
		name: 'q206',
		inputSchema: z.object({ id: z.string(), n206: z.number() })
	}),
	defineQueue({
		name: 'q207',
		inputSchema: z.object({ id: z.string(), n207: z.number() })
	}),
	defineQueue({
		name: 'q208',
		inputSchema: z.object({ id: z.string(), n208: z.number() })
	}),
	defineQueue({
		name: 'q209',
		inputSchema: z.object({ id: z.string(), n209: z.number() })
	}),
	defineQueue({
		name: 'q210',
		inputSchema: z.object({ id: z.string(), n210: z.number() })
	}),
	defineQueue({
		name: 'q211',
		inputSchema: z.object({ id: z.string(), n211: z.number() })
	}),
	defineQueue({
		name: 'q212',
		inputSchema: z.object({ id: z.string(), n212: z.number() })
	}),
	defineQueue({
		name: 'q213',
		inputSchema: z.object({ id: z.string(), n213: z.number() })
	}),
	defineQueue({
		name: 'q214',
		inputSchema: z.object({ id: z.string(), n214: z.number() })
	}),
	defineQueue({
		name: 'q215',
		inputSchema: z.object({ id: z.string(), n215: z.number() })
	}),
	defineQueue({
		name: 'q216',
		inputSchema: z.object({ id: z.string(), n216: z.number() })
	}),
	defineQueue({
		name: 'q217',
		inputSchema: z.object({ id: z.string(), n217: z.number() })
	}),
	defineQueue({
		name: 'q218',
		inputSchema: z.object({ id: z.string(), n218: z.number() })
	}),
	defineQueue({
		name: 'q219',
		inputSchema: z.object({ id: z.string(), n219: z.number() })
	}),
	defineQueue({
		name: 'q220',
		inputSchema: z.object({ id: z.string(), n220: z.number() })
	}),
	defineQueue({
		name: 'q221',
		inputSchema: z.object({ id: z.string(), n221: z.number() })
	}),
	defineQueue({
		name: 'q222',
		inputSchema: z.object({ id: z.string(), n222: z.number() })
	}),
	defineQueue({
		name: 'q223',
		inputSchema: z.object({ id: z.string(), n223: z.number() })
	}),
	defineQueue({
		name: 'q224',
		inputSchema: z.object({ id: z.string(), n224: z.number() })
	}),
	defineQueue({
		name: 'q225',
		inputSchema: z.object({ id: z.string(), n225: z.number() })
	}),
	defineQueue({
		name: 'q226',
		inputSchema: z.object({ id: z.string(), n226: z.number() })
	}),
	defineQueue({
		name: 'q227',
		inputSchema: z.object({ id: z.string(), n227: z.number() })
	}),
	defineQueue({
		name: 'q228',
		inputSchema: z.object({ id: z.string(), n228: z.number() })
	}),
	defineQueue({
		name: 'q229',
		inputSchema: z.object({ id: z.string(), n229: z.number() })
	}),
	defineQueue({
		name: 'q230',
		inputSchema: z.object({ id: z.string(), n230: z.number() })
	}),
	defineQueue({
		name: 'q231',
		inputSchema: z.object({ id: z.string(), n231: z.number() })
	}),
	defineQueue({
		name: 'q232',
		inputSchema: z.object({ id: z.string(), n232: z.number() })
	}),
	defineQueue({
		name: 'q233',
		inputSchema: z.object({ id: z.string(), n233: z.number() })
	}),
	defineQueue({
		name: 'q234',
		inputSchema: z.object({ id: z.string(), n234: z.number() })
	}),
	defineQueue({
		name: 'q235',
		inputSchema: z.object({ id: z.string(), n235: z.number() })
	}),
	defineQueue({
		name: 'q236',
		inputSchema: z.object({ id: z.string(), n236: z.number() })
	}),
	defineQueue({
		name: 'q237',
		inputSchema: z.object({ id: z.string(), n237: z.number() })
	}),
	defineQueue({
		name: 'q238',
		inputSchema: z.object({ id: z.string(), n238: z.number() })
	}),
	defineQueue({
		name: 'q239',
		inputSchema: z.object({ id: z.string(), n239: z.number() })
	}),
	defineQueue({
		name: 'q240',
		inputSchema: z.object({ id: z.string(), n240: z.number() })
	}),
	defineQueue({
		name: 'q241',
		inputSchema: z.object({ id: z.string(), n241: z.number() })
	}),
	defineQueue({
		name: 'q242',
		inputSchema: z.object({ id: z.string(), n242: z.number() })
	}),
	defineQueue({
		name: 'q243',
		inputSchema: z.object({ id: z.string(), n243: z.number() })
	}),
	defineQueue({
		name: 'q244',
		inputSchema: z.object({ id: z.string(), n244: z.number() })
	}),
	defineQueue({
		name: 'q245',
		inputSchema: z.object({ id: z.string(), n245: z.number() })
	}),
	defineQueue({
		name: 'q246',
		inputSchema: z.object({ id: z.string(), n246: z.number() })
	}),
	defineQueue({
		name: 'q247',
		inputSchema: z.object({ id: z.string(), n247: z.number() })
	}),
	defineQueue({
		name: 'q248',
		inputSchema: z.object({ id: z.string(), n248: z.number() })
	}),
	defineQueue({
		name: 'q249',
		inputSchema: z.object({ id: z.string(), n249: z.number() })
	}),
	defineQueue({
		name: 'q250',
		inputSchema: z.object({ id: z.string(), n250: z.number() })
	}),
	defineQueue({
		name: 'q251',
		inputSchema: z.object({ id: z.string(), n251: z.number() })
	}),
	defineQueue({
		name: 'q252',
		inputSchema: z.object({ id: z.string(), n252: z.number() })
	}),
	defineQueue({
		name: 'q253',
		inputSchema: z.object({ id: z.string(), n253: z.number() })
	}),
	defineQueue({
		name: 'q254',
		inputSchema: z.object({ id: z.string(), n254: z.number() })
	}),
	defineQueue({
		name: 'q255',
		inputSchema: z.object({ id: z.string(), n255: z.number() })
	}),
	defineQueue({
		name: 'q256',
		inputSchema: z.object({ id: z.string(), n256: z.number() })
	}),
	defineQueue({
		name: 'q257',
		inputSchema: z.object({ id: z.string(), n257: z.number() })
	}),
	defineQueue({
		name: 'q258',
		inputSchema: z.object({ id: z.string(), n258: z.number() })
	}),
	defineQueue({
		name: 'q259',
		inputSchema: z.object({ id: z.string(), n259: z.number() })
	}),
	defineQueue({
		name: 'q260',
		inputSchema: z.object({ id: z.string(), n260: z.number() })
	}),
	defineQueue({
		name: 'q261',
		inputSchema: z.object({ id: z.string(), n261: z.number() })
	}),
	defineQueue({
		name: 'q262',
		inputSchema: z.object({ id: z.string(), n262: z.number() })
	}),
	defineQueue({
		name: 'q263',
		inputSchema: z.object({ id: z.string(), n263: z.number() })
	}),
	defineQueue({
		name: 'q264',
		inputSchema: z.object({ id: z.string(), n264: z.number() })
	}),
	defineQueue({
		name: 'q265',
		inputSchema: z.object({ id: z.string(), n265: z.number() })
	}),
	defineQueue({
		name: 'q266',
		inputSchema: z.object({ id: z.string(), n266: z.number() })
	}),
	defineQueue({
		name: 'q267',
		inputSchema: z.object({ id: z.string(), n267: z.number() })
	}),
	defineQueue({
		name: 'q268',
		inputSchema: z.object({ id: z.string(), n268: z.number() })
	}),
	defineQueue({
		name: 'q269',
		inputSchema: z.object({ id: z.string(), n269: z.number() })
	}),
	defineQueue({
		name: 'q270',
		inputSchema: z.object({ id: z.string(), n270: z.number() })
	}),
	defineQueue({
		name: 'q271',
		inputSchema: z.object({ id: z.string(), n271: z.number() })
	}),
	defineQueue({
		name: 'q272',
		inputSchema: z.object({ id: z.string(), n272: z.number() })
	}),
	defineQueue({
		name: 'q273',
		inputSchema: z.object({ id: z.string(), n273: z.number() })
	}),
	defineQueue({
		name: 'q274',
		inputSchema: z.object({ id: z.string(), n274: z.number() })
	}),
	defineQueue({
		name: 'q275',
		inputSchema: z.object({ id: z.string(), n275: z.number() })
	}),
	defineQueue({
		name: 'q276',
		inputSchema: z.object({ id: z.string(), n276: z.number() })
	}),
	defineQueue({
		name: 'q277',
		inputSchema: z.object({ id: z.string(), n277: z.number() })
	}),
	defineQueue({
		name: 'q278',
		inputSchema: z.object({ id: z.string(), n278: z.number() })
	}),
	defineQueue({
		name: 'q279',
		inputSchema: z.object({ id: z.string(), n279: z.number() })
	}),
	defineQueue({
		name: 'q280',
		inputSchema: z.object({ id: z.string(), n280: z.number() })
	}),
	defineQueue({
		name: 'q281',
		inputSchema: z.object({ id: z.string(), n281: z.number() })
	}),
	defineQueue({
		name: 'q282',
		inputSchema: z.object({ id: z.string(), n282: z.number() })
	}),
	defineQueue({
		name: 'q283',
		inputSchema: z.object({ id: z.string(), n283: z.number() })
	}),
	defineQueue({
		name: 'q284',
		inputSchema: z.object({ id: z.string(), n284: z.number() })
	}),
	defineQueue({
		name: 'q285',
		inputSchema: z.object({ id: z.string(), n285: z.number() })
	}),
	defineQueue({
		name: 'q286',
		inputSchema: z.object({ id: z.string(), n286: z.number() })
	}),
	defineQueue({
		name: 'q287',
		inputSchema: z.object({ id: z.string(), n287: z.number() })
	}),
	defineQueue({
		name: 'q288',
		inputSchema: z.object({ id: z.string(), n288: z.number() })
	}),
	defineQueue({
		name: 'q289',
		inputSchema: z.object({ id: z.string(), n289: z.number() })
	}),
	defineQueue({
		name: 'q290',
		inputSchema: z.object({ id: z.string(), n290: z.number() })
	}),
	defineQueue({
		name: 'q291',
		inputSchema: z.object({ id: z.string(), n291: z.number() })
	}),
	defineQueue({
		name: 'q292',
		inputSchema: z.object({ id: z.string(), n292: z.number() })
	}),
	defineQueue({
		name: 'q293',
		inputSchema: z.object({ id: z.string(), n293: z.number() })
	}),
	defineQueue({
		name: 'q294',
		inputSchema: z.object({ id: z.string(), n294: z.number() })
	}),
	defineQueue({
		name: 'q295',
		inputSchema: z.object({ id: z.string(), n295: z.number() })
	}),
	defineQueue({
		name: 'q296',
		inputSchema: z.object({ id: z.string(), n296: z.number() })
	}),
	defineQueue({
		name: 'q297',
		inputSchema: z.object({ id: z.string(), n297: z.number() })
	}),
	defineQueue({
		name: 'q298',
		inputSchema: z.object({ id: z.string(), n298: z.number() })
	}),
	defineQueue({
		name: 'q299',
		inputSchema: z.object({ id: z.string(), n299: z.number() })
	}),
	defineQueue({
		name: 'q300',
		inputSchema: z.object({ id: z.string(), n300: z.number() })
	}),
	defineQueue({
		name: 'q301',
		inputSchema: z.object({ id: z.string(), n301: z.number() })
	}),
	defineQueue({
		name: 'q302',
		inputSchema: z.object({ id: z.string(), n302: z.number() })
	}),
	defineQueue({
		name: 'q303',
		inputSchema: z.object({ id: z.string(), n303: z.number() })
	}),
	defineQueue({
		name: 'q304',
		inputSchema: z.object({ id: z.string(), n304: z.number() })
	}),
	defineQueue({
		name: 'q305',
		inputSchema: z.object({ id: z.string(), n305: z.number() })
	}),
	defineQueue({
		name: 'q306',
		inputSchema: z.object({ id: z.string(), n306: z.number() })
	}),
	defineQueue({
		name: 'q307',
		inputSchema: z.object({ id: z.string(), n307: z.number() })
	}),
	defineQueue({
		name: 'q308',
		inputSchema: z.object({ id: z.string(), n308: z.number() })
	}),
	defineQueue({
		name: 'q309',
		inputSchema: z.object({ id: z.string(), n309: z.number() })
	}),
	defineQueue({
		name: 'q310',
		inputSchema: z.object({ id: z.string(), n310: z.number() })
	}),
	defineQueue({
		name: 'q311',
		inputSchema: z.object({ id: z.string(), n311: z.number() })
	}),
	defineQueue({
		name: 'q312',
		inputSchema: z.object({ id: z.string(), n312: z.number() })
	}),
	defineQueue({
		name: 'q313',
		inputSchema: z.object({ id: z.string(), n313: z.number() })
	}),
	defineQueue({
		name: 'q314',
		inputSchema: z.object({ id: z.string(), n314: z.number() })
	}),
	defineQueue({
		name: 'q315',
		inputSchema: z.object({ id: z.string(), n315: z.number() })
	}),
	defineQueue({
		name: 'q316',
		inputSchema: z.object({ id: z.string(), n316: z.number() })
	}),
	defineQueue({
		name: 'q317',
		inputSchema: z.object({ id: z.string(), n317: z.number() })
	}),
	defineQueue({
		name: 'q318',
		inputSchema: z.object({ id: z.string(), n318: z.number() })
	}),
	defineQueue({
		name: 'q319',
		inputSchema: z.object({ id: z.string(), n319: z.number() })
	}),
	defineQueue({
		name: 'q320',
		inputSchema: z.object({ id: z.string(), n320: z.number() })
	}),
	defineQueue({
		name: 'q321',
		inputSchema: z.object({ id: z.string(), n321: z.number() })
	}),
	defineQueue({
		name: 'q322',
		inputSchema: z.object({ id: z.string(), n322: z.number() })
	}),
	defineQueue({
		name: 'q323',
		inputSchema: z.object({ id: z.string(), n323: z.number() })
	}),
	defineQueue({
		name: 'q324',
		inputSchema: z.object({ id: z.string(), n324: z.number() })
	}),
	defineQueue({
		name: 'q325',
		inputSchema: z.object({ id: z.string(), n325: z.number() })
	}),
	defineQueue({
		name: 'q326',
		inputSchema: z.object({ id: z.string(), n326: z.number() })
	}),
	defineQueue({
		name: 'q327',
		inputSchema: z.object({ id: z.string(), n327: z.number() })
	}),
	defineQueue({
		name: 'q328',
		inputSchema: z.object({ id: z.string(), n328: z.number() })
	}),
	defineQueue({
		name: 'q329',
		inputSchema: z.object({ id: z.string(), n329: z.number() })
	}),
	defineQueue({
		name: 'q330',
		inputSchema: z.object({ id: z.string(), n330: z.number() })
	}),
	defineQueue({
		name: 'q331',
		inputSchema: z.object({ id: z.string(), n331: z.number() })
	}),
	defineQueue({
		name: 'q332',
		inputSchema: z.object({ id: z.string(), n332: z.number() })
	}),
	defineQueue({
		name: 'q333',
		inputSchema: z.object({ id: z.string(), n333: z.number() })
	}),
	defineQueue({
		name: 'q334',
		inputSchema: z.object({ id: z.string(), n334: z.number() })
	}),
	defineQueue({
		name: 'q335',
		inputSchema: z.object({ id: z.string(), n335: z.number() })
	}),
	defineQueue({
		name: 'q336',
		inputSchema: z.object({ id: z.string(), n336: z.number() })
	}),
	defineQueue({
		name: 'q337',
		inputSchema: z.object({ id: z.string(), n337: z.number() })
	}),
	defineQueue({
		name: 'q338',
		inputSchema: z.object({ id: z.string(), n338: z.number() })
	}),
	defineQueue({
		name: 'q339',
		inputSchema: z.object({ id: z.string(), n339: z.number() })
	}),
	defineQueue({
		name: 'q340',
		inputSchema: z.object({ id: z.string(), n340: z.number() })
	}),
	defineQueue({
		name: 'q341',
		inputSchema: z.object({ id: z.string(), n341: z.number() })
	}),
	defineQueue({
		name: 'q342',
		inputSchema: z.object({ id: z.string(), n342: z.number() })
	}),
	defineQueue({
		name: 'q343',
		inputSchema: z.object({ id: z.string(), n343: z.number() })
	}),
	defineQueue({
		name: 'q344',
		inputSchema: z.object({ id: z.string(), n344: z.number() })
	}),
	defineQueue({
		name: 'q345',
		inputSchema: z.object({ id: z.string(), n345: z.number() })
	}),
	defineQueue({
		name: 'q346',
		inputSchema: z.object({ id: z.string(), n346: z.number() })
	}),
	defineQueue({
		name: 'q347',
		inputSchema: z.object({ id: z.string(), n347: z.number() })
	}),
	defineQueue({
		name: 'q348',
		inputSchema: z.object({ id: z.string(), n348: z.number() })
	}),
	defineQueue({
		name: 'q349',
		inputSchema: z.object({ id: z.string(), n349: z.number() })
	}),
	defineQueue({
		name: 'q350',
		inputSchema: z.object({ id: z.string(), n350: z.number() })
	}),
	defineQueue({
		name: 'q351',
		inputSchema: z.object({ id: z.string(), n351: z.number() })
	}),
	defineQueue({
		name: 'q352',
		inputSchema: z.object({ id: z.string(), n352: z.number() })
	}),
	defineQueue({
		name: 'q353',
		inputSchema: z.object({ id: z.string(), n353: z.number() })
	}),
	defineQueue({
		name: 'q354',
		inputSchema: z.object({ id: z.string(), n354: z.number() })
	}),
	defineQueue({
		name: 'q355',
		inputSchema: z.object({ id: z.string(), n355: z.number() })
	}),
	defineQueue({
		name: 'q356',
		inputSchema: z.object({ id: z.string(), n356: z.number() })
	}),
	defineQueue({
		name: 'q357',
		inputSchema: z.object({ id: z.string(), n357: z.number() })
	}),
	defineQueue({
		name: 'q358',
		inputSchema: z.object({ id: z.string(), n358: z.number() })
	}),
	defineQueue({
		name: 'q359',
		inputSchema: z.object({ id: z.string(), n359: z.number() })
	}),
	defineQueue({
		name: 'q360',
		inputSchema: z.object({ id: z.string(), n360: z.number() })
	}),
	defineQueue({
		name: 'q361',
		inputSchema: z.object({ id: z.string(), n361: z.number() })
	}),
	defineQueue({
		name: 'q362',
		inputSchema: z.object({ id: z.string(), n362: z.number() })
	}),
	defineQueue({
		name: 'q363',
		inputSchema: z.object({ id: z.string(), n363: z.number() })
	}),
	defineQueue({
		name: 'q364',
		inputSchema: z.object({ id: z.string(), n364: z.number() })
	}),
	defineQueue({
		name: 'q365',
		inputSchema: z.object({ id: z.string(), n365: z.number() })
	}),
	defineQueue({
		name: 'q366',
		inputSchema: z.object({ id: z.string(), n366: z.number() })
	}),
	defineQueue({
		name: 'q367',
		inputSchema: z.object({ id: z.string(), n367: z.number() })
	}),
	defineQueue({
		name: 'q368',
		inputSchema: z.object({ id: z.string(), n368: z.number() })
	}),
	defineQueue({
		name: 'q369',
		inputSchema: z.object({ id: z.string(), n369: z.number() })
	}),
	defineQueue({
		name: 'q370',
		inputSchema: z.object({ id: z.string(), n370: z.number() })
	}),
	defineQueue({
		name: 'q371',
		inputSchema: z.object({ id: z.string(), n371: z.number() })
	}),
	defineQueue({
		name: 'q372',
		inputSchema: z.object({ id: z.string(), n372: z.number() })
	}),
	defineQueue({
		name: 'q373',
		inputSchema: z.object({ id: z.string(), n373: z.number() })
	}),
	defineQueue({
		name: 'q374',
		inputSchema: z.object({ id: z.string(), n374: z.number() })
	}),
	defineQueue({
		name: 'q375',
		inputSchema: z.object({ id: z.string(), n375: z.number() })
	}),
	defineQueue({
		name: 'q376',
		inputSchema: z.object({ id: z.string(), n376: z.number() })
	}),
	defineQueue({
		name: 'q377',
		inputSchema: z.object({ id: z.string(), n377: z.number() })
	}),
	defineQueue({
		name: 'q378',
		inputSchema: z.object({ id: z.string(), n378: z.number() })
	}),
	defineQueue({
		name: 'q379',
		inputSchema: z.object({ id: z.string(), n379: z.number() })
	}),
	defineQueue({
		name: 'q380',
		inputSchema: z.object({ id: z.string(), n380: z.number() })
	}),
	defineQueue({
		name: 'q381',
		inputSchema: z.object({ id: z.string(), n381: z.number() })
	}),
	defineQueue({
		name: 'q382',
		inputSchema: z.object({ id: z.string(), n382: z.number() })
	}),
	defineQueue({
		name: 'q383',
		inputSchema: z.object({ id: z.string(), n383: z.number() })
	}),
	defineQueue({
		name: 'q384',
		inputSchema: z.object({ id: z.string(), n384: z.number() })
	}),
	defineQueue({
		name: 'q385',
		inputSchema: z.object({ id: z.string(), n385: z.number() })
	}),
	defineQueue({
		name: 'q386',
		inputSchema: z.object({ id: z.string(), n386: z.number() })
	}),
	defineQueue({
		name: 'q387',
		inputSchema: z.object({ id: z.string(), n387: z.number() })
	}),
	defineQueue({
		name: 'q388',
		inputSchema: z.object({ id: z.string(), n388: z.number() })
	}),
	defineQueue({
		name: 'q389',
		inputSchema: z.object({ id: z.string(), n389: z.number() })
	}),
	defineQueue({
		name: 'q390',
		inputSchema: z.object({ id: z.string(), n390: z.number() })
	}),
	defineQueue({
		name: 'q391',
		inputSchema: z.object({ id: z.string(), n391: z.number() })
	}),
	defineQueue({
		name: 'q392',
		inputSchema: z.object({ id: z.string(), n392: z.number() })
	}),
	defineQueue({
		name: 'q393',
		inputSchema: z.object({ id: z.string(), n393: z.number() })
	}),
	defineQueue({
		name: 'q394',
		inputSchema: z.object({ id: z.string(), n394: z.number() })
	}),
	defineQueue({
		name: 'q395',
		inputSchema: z.object({ id: z.string(), n395: z.number() })
	}),
	defineQueue({
		name: 'q396',
		inputSchema: z.object({ id: z.string(), n396: z.number() })
	}),
	defineQueue({
		name: 'q397',
		inputSchema: z.object({ id: z.string(), n397: z.number() })
	}),
	defineQueue({
		name: 'q398',
		inputSchema: z.object({ id: z.string(), n398: z.number() })
	}),
	defineQueue({
		name: 'q399',
		inputSchema: z.object({ id: z.string(), n399: z.number() })
	})
] as const
