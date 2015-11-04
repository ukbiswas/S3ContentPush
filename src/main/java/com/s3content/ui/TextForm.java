package com.s3content.ui;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class TextForm extends JPanel {

  private JTextField[] fields;

  // Create a form with the specified labels, tooltips, and sizes.
  public TextForm(String[] labels, char[] mnemonics, int[] widths, String[] tips) {
    super(new BorderLayout());
    JPanel labelPanel = new JPanel(new GridLayout(labels.length, 1));
    JPanel fieldPanel = new JPanel(new GridLayout(labels.length, 1));
    add(labelPanel, BorderLayout.WEST);
    add(fieldPanel, BorderLayout.CENTER);
    fields = new JTextField[labels.length];

    for (int i = 0; i < labels.length; i += 1) {
      fields[i] = new JTextField();
      if (i < tips.length)
        fields[i].setToolTipText(tips[i]);
      if (i < widths.length)
        fields[i].setColumns(widths[i]);

      JLabel lab = new JLabel(labels[i], JLabel.RIGHT);
      lab.setLabelFor(fields[i]);
      if (i < mnemonics.length)
        lab.setDisplayedMnemonic(mnemonics[i]);

      labelPanel.add(lab);
      JPanel p = new JPanel(new FlowLayout(FlowLayout.LEFT));
      p.add(fields[i]);
      fieldPanel.add(p);
    }
  }

  public String getText(int i) {
    return (fields[i].getText());
  }

  public static void main(String[] args) {
    String[] labels = { "Source Access Key", "Source Secret Key", "Dest Access Key", "Dest Secret Key", "Source Bucket", "Dest Bucket","Source Environment","Dest Environment" };
    char[] mnemonics = { 'F', 'M', 'L', 'A' };
    int[] widths = { 15, 15, 15, 15,15,15,15,15 };
    String[] descs = { "Source Bucket Access Key", "Source Bucket Secret Key", "Dest Bucket Access Key", "Dest Bucket Secret Key" };

    final TextForm form = new TextForm(labels, mnemonics, widths, descs);

    JButton submit = new JButton("Submit Form");

    submit.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        System.out.println(form.getText(0) + " " + form.getText(1) + ". " + form.getText(2)
            + ", age " + form.getText(3));
      }
    });

    JFrame f = new JFrame("Text Form Example");
    f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    f.getContentPane().add(form, BorderLayout.NORTH);
    JPanel p = new JPanel();
    p.add(submit);
    f.getContentPane().add(p, BorderLayout.SOUTH);
    f.pack();
    f.setVisible(true);
  }
}
