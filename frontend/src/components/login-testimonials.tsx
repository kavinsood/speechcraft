"use client";

import { AnimatePresence, motion } from "framer-motion";
import { useEffect, useState } from "react";

type Testimonial = {
  name?: string;
  title?: string;
  content?: string;
  highlighted: string;
  firstPart: string;
  secondPart: string;
  // Which part actually contains the `highlighted` text. Some entries'
  // highlighted quote straddles the firstPart/secondPart boundary, so this
  // is set explicitly per-entry rather than inferred.
  highlightPart: "first" | "second";
};

const testimonials: Testimonial[] = [
  {
    highlighted: "We went from two months of data prep down to ten days.",
    firstPart: "We went from two months of data prep down to ten days",
    secondPart:
      ". We had studio recordings, raw interviews, and archival audio we'd never been able to use. Speechcraft ingested all of it and handed us a clean dataset with a full trail back to every source file.",
    highlightPart: "first",
  },
  {
    highlighted:
      "I didn't realise how much of my training data was quietly wrong until I could actually see it.",
    firstPart:
      "I didn't realise how much of my training data was quietly wrong until I could actually see it",
    secondPart:
      ". Speechcraft surfaced clips cut mid-word, mismatched-transcripts and background bleed. The voice I shipped after fixing that was night and day compared to anything I'd trained before.",
    highlightPart: "first",
  },
  {
    highlighted:
      "Our model quality improved more from fixing the data than from anything we had changed about the model itself.",
    firstPart:
      "I was manually pulling clips from a 40-hour podcast. Speechcraft processed the entire archive and every clip came back clean enough to train on",
    secondPart:
      ". Our model quality improved more from fixing the data than from anything we had changed about the model itself.",
    highlightPart: "second",
  },
  {
    highlighted: "We used that budget for compute instead.",
    firstPart:
      "We were about to hire a freelancer for three months to clean and label our audio archive. Speechcraft got us to the same place in four days, and the output was more consistent than what a human working manually would have produced",
    secondPart: ". We used that budget for compute instead.",
    highlightPart: "second",
  },
  {
    highlighted:
      "I had a clean labelled dataset before my advisor expected me to have finished data collection.",
    firstPart:
      "I came into this expecting to spend most of the fellowship building data infrastructure before I could start my actual research",
    secondPart:
      ". Speechcraft collapsed that into a week. I had a clean labelled dataset before my advisor expected me to have finished data collection.",
    highlightPart: "second",
  },
  {
    content:
      "Unhappy with results, we switched base models mid-project. Normally, differing format requirements meant redoing all data preparation. With Speechcraft, we simply changed the export target and recompiled. The dataset was ready in minutes. We didn't lose a single annotation or re-review any clips.",
    highlighted:
      "The dataset was ready in minutes. We didn't lose a single annotation or re-review any clips.",
    firstPart:
      "Unhappy with results, we switched base models mid-project. Normally, differing format requirements meant redoing all data preparation. With Speechcraft, we simply changed the export target and recompiled.",
    secondPart:
      " The dataset was ready in minutes. We didn't lose a single annotation or re-review any clips.",
    highlightPart: "second",
  },
  {
    content:
      "Before this, when my model output sounded wrong, I'd have to trace it back manually. With Speechcraft I know exactly which recording fed which clip, who reviewed it, and what state it's in before anything reaches training. This has changed how fast I can iterate.",
    highlighted:
      "I know exactly which recording fed which clip, who reviewed it, and what state it's in before anything reaches training.",
    firstPart:
      "Before this, when my model output sounded wrong, I'd have to trace it back manually. With Speechcraft I know exactly which recording fed which clip, who reviewed it, and what state it's in before anything reaches training.",
    secondPart:
      " That sounds like a small thing. This has changed how fast I can iterate.",
    highlightPart: "first",
  },
  {
    content:
      "Our previous workflow for preparing a new voice model was about six weeks if everything went right, which it usually didn't. Someone would run a script wrong, a step would time out and we'd lose the work. We did the same scope of work in Speechcraft in eight days. The time we saved on the first project alone covered the time it took to learn the tool.",
    highlighted: "We did the same scope of work in eight days.",
    firstPart:
      "Our previous workflow for preparing a new voice model was about six weeks if everything went right, which it usually didn't. Someone would run a script wrong, a step would time out and we'd lose the work. We did the same scope of work in Speechcraft in eight days.",
    secondPart:
      " We did the same scope of work in eight days. The time we saved on the first project alone covered the time it took to learn the tool.",
    highlightPart: "second",
  },
];

export default function LoginTestimonials() {
  const [currentTestimonial, setCurrentTestimonial] = useState(0);

  useEffect(() => {
    // Set random starting index only on client to avoid hydration mismatch
    setCurrentTestimonial(Math.floor(Math.random() * testimonials.length));

    const interval = setInterval(() => {
      setCurrentTestimonial((prev) => (prev + 1) % testimonials.length);
    }, 6000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="relative h-64 flex items-center justify-center">
      <AnimatePresence mode="wait">
        <motion.div
          key={currentTestimonial}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.3 }}
          className="text-center space-y-4"
        >
          {/* Quote - First */}
          <motion.div
            initial={{ opacity: 0, filter: "blur(2px)", y: 10 }}
            animate={{ opacity: 1, filter: "blur(0px)", y: 0 }}
            transition={{ duration: 0.6, delay: 0.1, ease: "easeOut" }}
            className="relative max-w-md mx-auto"
          >
            {/* Large opening quote */}
            <div className="absolute left-1/2 top-1/2 transform -translate-x-1/2 -translate-y-1/2 opacity-[0.02]">
              <svg
                width="220"
                height="220"
                viewBox="0 0 6 5"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                className="w-[220px] h-[220px] object-contain"
              >
                <path
                  d="M4.54533 4.828C4.16133 4.828 3.84333 4.684 3.59133 4.396C3.35133 4.108 3.23133 3.712 3.23133 3.208C3.23133 2.644 3.41133 2.104 3.77133 1.588C4.13133 1.072 4.68933 0.615999 5.44533 0.219999L5.76933 0.669999C5.12133 1.054 4.68933 1.438 4.47333 1.822C4.25733 2.206 4.14933 2.626 4.14933 3.082L3.68133 3.82C3.68133 3.52 3.77133 3.28 3.95133 3.1C4.14333 2.908 4.38333 2.812 4.67133 2.812C4.94733 2.812 5.18133 2.902 5.37333 3.082C5.56533 3.262 5.66133 3.502 5.66133 3.802C5.66133 4.09 5.55933 4.336 5.35533 4.54C5.15133 4.732 4.88133 4.828 4.54533 4.828ZM1.50333 4.828C1.11933 4.828 0.801328 4.684 0.549328 4.396C0.309328 4.108 0.189328 3.712 0.189328 3.208C0.189328 2.644 0.369328 2.104 0.729328 1.588C1.08933 1.072 1.64733 0.615999 2.40333 0.219999L2.72733 0.669999C2.07933 1.054 1.64733 1.438 1.43133 1.822C1.21533 2.206 1.10733 2.626 1.10733 3.082L0.639328 3.82C0.639328 3.52 0.729328 3.28 0.909328 3.1C1.10133 2.908 1.34133 2.812 1.62933 2.812C1.90533 2.812 2.13933 2.902 2.33133 3.082C2.52333 3.262 2.61933 3.502 2.61933 3.802C2.61933 4.09 2.51733 4.336 2.31333 4.54C2.10933 4.732 1.83933 4.828 1.50333 4.828Z"
                  fill="white"
                />
              </svg>
            </div>
            <p className="font-sans text-xl text-white/40 leading-relaxed pl-4">
              {(() => {
                const testimonial = testimonials[currentTestimonial];
                const firstPart = testimonial?.firstPart || "";
                const secondPart = testimonial?.secondPart || "";
                const startsWithPunctuation =
                  secondPart.startsWith(".") || secondPart.startsWith(",");
                const punctuation = startsWithPunctuation ? secondPart[0] : ".";
                const secondPartWithoutPunctuation = startsWithPunctuation
                  ? secondPart.slice(1)
                  : secondPart;

                return testimonial?.highlightPart === "first" ? (
                  <>
                    <span className="text-white">
                      "{firstPart}
                      {punctuation}
                    </span>
                    {secondPartWithoutPunctuation}"
                  </>
                ) : (
                  <>
                    "{firstPart}
                    {punctuation}
                    <span className="text-white">
                      {secondPartWithoutPunctuation}"
                    </span>
                  </>
                );
              })()}
            </p>
          </motion.div>

          {/* Name and Title - Second (only when provided) */}
          {testimonials[currentTestimonial]?.name && (
            <motion.p
              initial={{ opacity: 0, filter: "blur(2px)", y: 10 }}
              animate={{ opacity: 1, filter: "blur(0px)", y: 0 }}
              transition={{ duration: 0.6, delay: 0.3, ease: "easeOut" }}
              className="font-sans text-xs text-white/40"
            >
              {testimonials[currentTestimonial]?.name}
              {testimonials[currentTestimonial]?.title
                ? `, ${testimonials[currentTestimonial]?.title}`
                : null}
            </motion.p>
          )}
        </motion.div>
      </AnimatePresence>
    </div>
  );
}
