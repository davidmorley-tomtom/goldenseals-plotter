import numpy as np
import os
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve, confusion_matrix, ConfusionMatrixDisplay
from PlotSQLQuery import PlotSQLQuery
from PostgresHandler import PostgresHandler


class PlotsGoldensealsKR1:
	def __init__(self, opts_dict: dict, pg: PostgresHandler):
		self._pg = pg
		self._opts_dict = opts_dict
		sql_store = PlotSQLQuery(self._opts_dict)

		# The hex_to_int function
		self._pg.execute_query(sql_store.get_hex2dec_function_sql())

		# The runid if needed
		self.run_id = self._opts_dict['headers_correlationid']
		if self.run_id == '':
			self.run_id = self._pg.execute_select_query(sql_store.get_most_recent_run())[0][0]

		# The joined prediction-moderation dataframe
		self._df = self._pg.execute_query_to_pandas(sql_store.get_validation_table_sql(self.run_id))

	def plot_confusion_matrix(self, save: bool = False):
		df = self._df[self._df['obsv'] != 'UNKNOWN'].dropna()
		labs = ['OPEN_POS', 'OPEN_NEG', 'OPEN_BOTH', 'CLOSED']
		cm = confusion_matrix(df['obsv'], df['pred'], labels=labs)
		cmap = plt.cm.get_cmap('Reds')
		disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=labs)
		disp.plot(cmap=cmap, xticks_rotation='vertical')
		plt.tight_layout()
		if save:
			self.save_plot('confusion_matrix.png')
		plt.show()

	def plot_precision_recall(self, save: bool = False):
		df = self._df[self._df['obsv'] != 'UNKNOWN'].dropna()

		# Optional, Remove predicted CLOSED, can make the plot go strange
		# because CLOSED is often certain at prob = 1.0 and is a big error if not moderated the same
		# so the curve will make a series of big steps

		df = df[df['pred'] != 'CLOSED']

		# Collapse problem to one vs. rest
		# Not ideal as should work out PR on each class and average for final plot,
		# but the system only gives 'body_confidence' of single predicted class
		df['y_bin'] = df.apply(lambda x: 1 if (x['pred'] == x['obsv']) else 0, axis=1)
		p, r, t = precision_recall_curve(df['y_bin'].ravel(), df['body_confidence'].ravel())

		coverage_pc = (len(df) / len(self._df)) * 100
		r_stable = np.insert(r * coverage_pc, 0, coverage_pc)
		p_stable = np.insert(p, 0, 0)

		best_r = 0
		best_model_threshold = 0
		for p_, r_, t_ in zip(p, r, t):
			if p_ >= self._opts_dict['threshold'] and r_ > best_r:
				best_r = r_
				best_model_threshold = t_
		print('Best prec-recall probability threshold: {0:0.4f}'.format(best_model_threshold))

		labels = []
		lines = []
		fig = plt.figure(figsize=(14, 7))
		fig.subplots_adjust(bottom=0.15)
		plt.xlim([0.0, 100])
		plt.ylim([0.0, 1.01])
		plt.xticks(np.arange(0, 101, 10))
		plt.axvline(best_r * coverage_pc, c='blue', alpha=0.2, ls=':')
		plt.axhline(0.9, c='blue', alpha=0.2, ls=':')
		l, = plt.plot(r_stable, p_stable, color='gold', lw=2)
		lines.append(l)
		labels.append('{0:0.2f}% coverage at {1:0.1f} precision'.format(best_r * coverage_pc, self._opts_dict['threshold']))
		plt.title('Moderated stable set model coverage')
		plt.xlabel('% Stable set ({0} roads)'.format(len(self._df)))
		plt.ylabel('Precision')
		plt.legend(lines, labels, loc=(0, -.18), prop=dict(size=14))
		plt.tight_layout()
		if save:
			self.save_plot('precision_recall.png')
		plt.show()

	def save_plot(self, fn: str):
		try:
			plt.savefig(os.path.join(self._opts_dict['out_dir'], fn))
		except ValueError as e:
			print("Error saving plot:", e)
