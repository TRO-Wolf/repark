import java.io.*;

public class JavaSweep {
    static final String[][] T = {
        {"Α", "Σ"}, {"ΑΣ", "Β"}, {"", "Σ"}, {"ΑΣ", ""},
        {"Α-", "Σ"}, {"Α1", "Σ"}, {"Α", "1Σ"}, {"中", "Σ"},
        {"ΑΣ", "1Β"}, {"Α", "#Σ"}, {"ΑΣ1", "Β"},
        {"ΑΣ-", "Β"}, {"$", "Σ"}, {"Α", "́Σ"}, {"", "ΑΣ"},
        {"Α.", "Σ"}, {"ΑΣ", "́Β"}, {"Σ", ""}, {"ΑΣ.", "Β"},
        {"ΑΣ", "1"}, {" ", "Σ"}, {"Α1,", "1Σ"},
    };

    static String sig(String s) {
        StringBuilder b = new StringBuilder();
        s.codePoints().forEach(cp -> {
            if (cp == 0x3c3) b.append('s');
            else if (cp == 0x3c2) b.append('f');
        });
        return b.toString();
    }

    public static void main(String[] a) throws Exception {
        try (PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(a[0])))) {
            for (int c = 0; c <= 0x10FFFF; c++) {
                if (c >= 0xd800 && c <= 0xdfff) continue;
                String ch = new String(Character.toChars(c));
                StringBuilder line = new StringBuilder(Integer.toHexString(c));
                for (String[] t : T) {
                    String x = t[1].equals("#Σ") ? t[0] + ch + ch + "Σ" : t[0] + ch + t[1];
                    line.append('\t').append(sig(x.toLowerCase()));
                }
                w.println(line);
            }
        }
    }
}
