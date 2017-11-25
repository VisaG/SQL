/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.scu.IMDB.framework;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.WindowConstants;

/**
 *
 * @author pawan
 */
//parent JFrame
public abstract class MyFrame extends JFrame {

    private String TITLE = "IMBD Movie Portal";


    public MyFrame() {
       
            setTitle(TITLE);
            setResizable(false);

            WindowListener exitListener = new WindowAdapter() {
                @Override
                public void windowClosing(WindowEvent e) {
                    int confirm = JOptionPane.showOptionDialog(
                            null, "Are you sure to close the application?",
                            "Exit Confirmation", JOptionPane.YES_NO_OPTION,
                            JOptionPane.QUESTION_MESSAGE, null, null, null);
                    if (confirm == JOptionPane.YES_OPTION) {
                        try {
                            setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
                            
                        } catch (NullPointerException ex) {
                           
                        } finally {
                            System.exit(0);
                        }
                    } else {
                        setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
                    }
                }
            };
            this.addWindowListener(exitListener);
            pack();
        
    }

    public abstract void reset();
}
